//! Distributed SQL plan groundwork (Stage 3J, spec section 12.10).
//!
//! This module is the planning and coordination seam for tablet-distributed
//! queries. It deliberately depends on small local metadata traits
//! ([`TabletLocator`], [`ClusterMetadata`]) instead of `mongreldb-cluster`, so
//! the query crate stays free of the cluster dependency; the cluster wave
//! adapts `mongreldb_cluster::tablet` routing metadata onto these traits.
//!
//! # What lives here
//!
//! * [`DistributedPlan`] — the plan structure from spec section 12.10: pinned
//!   [`QueryId`] + [`MetadataVersion`], [`PlanFragment`]s with tablet
//!   assignments, and [`ExchangeDescriptor`] edges between them.
//! * [`distribute`] — the planner. It lowers a [`LogicalPlanLite`] tree (the
//!   simplified input IR) onto tablets: colocated joins when both sides share
//!   partitioning and layout, broadcast joins when the small side fits under a
//!   byte threshold, repartition joins otherwise. Every fragment carries
//!   estimated rows/bytes and a maximum spill allowance (spec section 12.10).
//! * Distributed top-k — [`merge_top_k`] / [`exact_top_k`] are pure functions
//!   implementing the deterministic coordinator merge (final score descending,
//!   tablet id ascending, [`RowId`] ascending) with adaptive refill when a
//!   tablet's unseen-score bound shows it could still contribute winners.
//! * Execution skeleton — [`FragmentExecutor`], [`FragmentTransport`], and the
//!   [`Coordinator`] runtime: per-fragment resource reservation, cancellation
//!   fan-out wired to the existing [`SqlQueryRegistry`](crate::SqlQueryRegistry),
//!   worker lease expiry that cleans abandoned fragments, and real in-memory
//!   merge operators (k-way [`MergeSort`](FragmentOperator::MergeSort),
//!   [`FinalAggregate`](FragmentOperator::FinalAggregate) combine, distributed
//!   top-k).
//! * Remote execution — [`RemoteFragmentTransport`] and
//!   [`RemoteFragmentEndpoint`] use a versioned, bounded Arrow IPC pull
//!   protocol. One batch per pull provides backpressure; query-scoped
//!   cancellation and adaptive top-k refill cross the same authenticated
//!   node-internal RPC carrier.
//!
//! # Integration point with DataFusion
//!
//! [`DataFusionDistributedPlanner::lower`] converts a real
//! `datafusion::logical_expr::LogicalPlan` (produced by
//! [`MongrelSession`](crate::MongrelSession) after catalog/schema resolution)
//! onto [`LogicalPlanLite`] and hands the tree to [`distribute`]. Supported
//! operators: TableScan, Projection (column pushdown), Filter (pushdown onto
//! scans as typed predicate text for worker evaluation), Aggregate, Sort,
//! Limit, equi-Join, and Union. Every other DataFusion node is rejected with
//! [`DistributedError::Unsupported`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow::compute::{concat_batches, interleave, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::row::{RowConverter, SortField};
use arrow::util::display::array_value_to_string;
use bincode::Options;
use futures::stream::{self, BoxStream, FuturesUnordered, StreamExt};
use mongreldb_core::{CancellationReason, ExecutionControl, RowId};
use mongreldb_types::ids::{MetadataVersion, QueryId, TabletId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::query_registry::{CancelOutcome, RegisteredSqlQuery, SqlQueryOptions, SqlQueryRegistry};

/// Small-side byte threshold below which a join is planned as a broadcast
/// join (spec section 12.10: "broadcast small side").
pub const DEFAULT_BROADCAST_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

/// Default per-fragment maximum spill allowance (spec section 12.10: "every
/// plan includes ... a maximum spill allowance"). Bound to the core
/// [`mongreldb_core::SpillManager`] via [`FragmentControl::begin_spill`].
pub const DEFAULT_MAX_SPILL_BYTES_PER_FRAGMENT: u64 = 256 * 1024 * 1024;
/// Maximum server-issued authorization envelope forwarded to a worker.
pub const MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES: usize = 64 * 1024;

/// Rows per `RecordBatch` emitted by coordinator merge operators.
const COORDINATOR_OUTPUT_BATCH_ROWS: usize = 8_192;

/// Name of the unsigned row-id column every scored (top-k) stream carries.
/// Tablet scans feeding a distributed top-k must project this column; it is
/// stripped from the coordinator's final top-k output.
pub const TOPK_ROWID_COLUMN: &str = "__rowid";

/// Errors raised by distributed planning and coordination.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum DistributedError {
    /// The locator or metadata has no entry for the table.
    #[error("unknown table `{0}`")]
    UnknownTable(String),
    /// The table's layout has no tablets to scan.
    #[error("table `{table}` has no tablets in its layout")]
    EmptyLayout { table: String },
    /// The input IR or a computed plan is not well-formed.
    #[error("invalid plan: {0}")]
    InvalidPlan(String),
    /// A fragment's resource reservation was denied.
    #[error("fragment {fragment_id} resource reservation denied: {reason}")]
    Reservation { fragment_id: u32, reason: String },
    /// A fragment failed on its worker.
    #[error("fragment {fragment_id} failed on worker {worker}: {message}")]
    FragmentExecution {
        fragment_id: u32,
        worker: String,
        message: String,
    },
    /// The query was cancelled; carries the first observed reason.
    #[error("distributed query cancelled: {0:?}")]
    Cancelled(CancellationReason),
    /// A groundwork boundary was crossed (documented later-wave work).
    #[error("unsupported in this wave: {0}")]
    Unsupported(String),
    /// Arrow kernel failure inside a merge operator.
    #[error("arrow error: {0}")]
    Arrow(String),
    /// A remote fragment transport call failed.
    #[error("remote fragment transport error: {0}")]
    RemoteTransport(String),
    /// A remote fragment peer violated the versioned wire contract.
    #[error("remote fragment protocol error: {0}")]
    RemoteProtocol(String),
}

/// Result alias for distributed planning and coordination.
pub type DistributedResult<T> = Result<T, DistributedError>;

impl From<arrow::error::ArrowError> for DistributedError {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::Arrow(error.to_string())
    }
}

/// FNV-1a 64-bit hash (same convention as the cluster routing code).
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Cluster metadata traits (the dependency-free seam)
// ---------------------------------------------------------------------------

/// How one table's rows map onto tablets — the planner's dependency-free
/// mirror of `mongreldb_cluster::tablet::Partitioning` (spec section 12.2).
/// The cluster wave adapts the cluster type onto this enum one-to-one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionSpec {
    /// Hash of the declared columns into a fixed bucket space.
    Hash { columns: Vec<String>, buckets: u32 },
    /// Lexicographic ranges over the declared columns.
    Range { columns: Vec<String> },
    /// One bucket space per tenant.
    Tenant {
        column: String,
        buckets_per_tenant: u32,
    },
    /// Time-bucketed ranges over the timestamp column.
    TimeRange { column: String },
    /// Single-tablet (unpartitioned) table.
    Unpartitioned,
}

impl PartitionSpec {
    /// The declared partition-key columns, in key order.
    pub fn partition_columns(&self) -> Vec<&str> {
        match self {
            Self::Hash { columns, .. } | Self::Range { columns } => {
                columns.iter().map(String::as_str).collect()
            }
            Self::Tenant { column, .. } | Self::TimeRange { column } => vec![column.as_str()],
            Self::Unpartitioned => Vec::new(),
        }
    }

    /// True when two tables partition identically, so equal partition keys
    /// land on the same tablet and a join on the partition key can be planned
    /// colocated (spec section 12.10: "colocated join"). Two unpartitioned
    /// tables are trivially colocated (both live on their single tablet).
    pub fn colocated_with(&self, other: &PartitionSpec) -> bool {
        match (self, other) {
            (Self::Unpartitioned, Self::Unpartitioned) => true,
            (Self::Unpartitioned, _) | (_, Self::Unpartitioned) => false,
            _ => self == other,
        }
    }
}

/// Planner statistics for one table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableStats {
    /// Estimated live row count.
    pub row_count: u64,
    /// Estimated total size in bytes.
    pub total_bytes: u64,
}

/// Resolves which tablets serve one table. Defined here (not in
/// `mongreldb-cluster`) so the query crate stays free of the cluster
/// dependency; the cluster wave binds `TabletLayout`/routing metadata onto
/// this trait.
pub trait TabletLocator: Send + Sync {
    /// The tablets serving `table`, in deterministic layout order.
    fn tablets_for_table(&self, table: &str) -> DistributedResult<Vec<TabletId>>;
    /// How `table` is partitioned across those tablets.
    fn partitioning(&self, table: &str) -> DistributedResult<PartitionSpec>;
}

/// Control-plane metadata the planner pins against (spec section 12.10:
/// "coordinator plans using metadata version").
pub trait ClusterMetadata: Send + Sync {
    /// The control-plane metadata version this plan is pinned to.
    fn metadata_version(&self) -> MetadataVersion;
    /// Planner statistics for one table.
    fn table_stats(&self, table: &str) -> DistributedResult<TableStats>;
}

// ---------------------------------------------------------------------------
// Input IR (LogicalPlanLite)
// ---------------------------------------------------------------------------

/// One join-key equality pair (`left.col = right.col`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinKey {
    /// Column name on the left input.
    pub left: String,
    /// Column name on the right input.
    pub right: String,
}

/// One sort key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    /// Column name.
    pub column: String,
    /// True for `ORDER BY ... DESC`.
    pub descending: bool,
}

/// One aggregate expression.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateExpr {
    /// The aggregate function.
    pub function: AggregateFunction,
    /// The aggregated column; `None` only for `COUNT(*)`.
    pub column: Option<String>,
}

/// Supported aggregate functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    /// Row count (`column == None`) or non-null count.
    Count,
    /// Numeric sum.
    Sum,
    /// Numeric minimum.
    Min,
    /// Numeric maximum.
    Max,
    /// Numeric average.
    Avg,
}

impl AggregateFunction {
    fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
        }
    }
}

/// The simplified logical IR [`distribute`] lowers onto tablets.
///
/// This is the seam documented at the module level: the DataFusion
/// integration wave lowers real DataFusion plans onto this enum; this wave
/// plans from it directly.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LogicalPlanLite {
    /// Base-table scan with an optional opaque predicate and projection.
    Scan {
        /// Table name.
        table: String,
        /// Opaque predicate text (pushed down verbatim; not interpreted in
        /// this wave).
        predicate: Option<String>,
        /// Projected column names; empty = all columns.
        projection: Vec<String>,
    },
    /// Grouped aggregation.
    Aggregate {
        /// Input subtree.
        input: Box<LogicalPlanLite>,
        /// Group-by columns.
        group_by: Vec<String>,
        /// Aggregate expressions.
        aggregates: Vec<AggregateExpr>,
    },
    /// Equi-join.
    Join {
        /// Left input subtree.
        left: Box<LogicalPlanLite>,
        /// Right input subtree.
        right: Box<LogicalPlanLite>,
        /// Equality key pairs.
        on: Vec<JoinKey>,
    },
    /// Sort with an optional limit (`ORDER BY ... [LIMIT k]`).
    Sort {
        /// Input subtree.
        input: Box<LogicalPlanLite>,
        /// Sort keys, most significant first.
        keys: Vec<SortKey>,
        /// Optional limit.
        limit: Option<usize>,
    },
    /// Limit without an ordering.
    Limit {
        /// Input subtree.
        input: Box<LogicalPlanLite>,
        /// Row limit.
        limit: usize,
    },
    /// Concatenate inputs with identical schemas (`UNION ALL` shape).
    Union {
        /// Input subtrees (at least two).
        inputs: Vec<LogicalPlanLite>,
    },
}

// ---------------------------------------------------------------------------
// Plan model (spec section 12.10)
// ---------------------------------------------------------------------------

/// Plan-local fragment identifier (index into [`DistributedPlan::fragments`]).
pub type FragmentId = u32;
/// Plan-local exchange identifier (index into [`DistributedPlan::exchanges`]).
pub type ExchangeId = u32;

/// Where one fragment executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FragmentAssignment {
    /// On the worker serving this tablet's data.
    Tablet(TabletId),
    /// On the query coordinator (gateway) itself.
    Coordinator,
}

/// Which join input is the broadcast (build) side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildSide {
    /// The left input is broadcast.
    Left,
    /// The right input is broadcast.
    Right,
}

/// One physical operator inside a fragment (spec section 12.10).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FragmentOperator {
    /// Scan one tablet's slice of a table.
    TabletScan {
        /// Table name.
        table: String,
        /// Opaque pushed-down predicate text.
        predicate: Option<String>,
        /// Projected columns; empty = all.
        projection: Vec<String>,
    },
    /// Receive one exchange edge from a producer fragment.
    RemoteExchangeSource {
        /// The exchange edge this source consumes.
        exchange: ExchangeId,
    },
    /// Emit this fragment's output onto one exchange edge.
    RemoteExchangeSink {
        /// The exchange edge this sink feeds.
        exchange: ExchangeId,
    },
    /// Per-tablet partial aggregation (pre-shuffle combine).
    PartialAggregate {
        /// Group-by columns.
        group_by: Vec<String>,
        /// Aggregate expressions.
        aggregates: Vec<AggregateExpr>,
    },
    /// Coordinator-side combine of partial aggregates.
    FinalAggregate {
        /// Group-by columns.
        group_by: Vec<String>,
        /// Aggregate expressions.
        aggregates: Vec<AggregateExpr>,
    },
    /// Hash join over colocated inputs (no exchange needed).
    DistributedHashJoin {
        /// Equality key pairs.
        on: Vec<JoinKey>,
    },
    /// Join where the small build side is broadcast to every big-side
    /// fragment.
    BroadcastJoin {
        /// Equality key pairs.
        on: Vec<JoinKey>,
        /// Which input is broadcast.
        build_side: BuildSide,
    },
    /// Join after both sides are hash-repartitioned on the join keys.
    RepartitionJoin {
        /// Equality key pairs.
        on: Vec<JoinKey>,
    },
    /// Producer side: local bounded sort. Coordinator side: deterministic
    /// k-way merge of sorted streams.
    MergeSort {
        /// Sort keys.
        keys: Vec<SortKey>,
        /// Optional row limit applied after the sort/merge.
        limit: Option<usize>,
    },
    /// Producer side: bounded local top-k plus tie information. Coordinator
    /// side: deterministic merge (score desc, tablet asc, [`RowId`] asc) with
    /// adaptive refill (spec section 12.10).
    DistributedTopK {
        /// Number of winners.
        k: usize,
        /// The (single, descending) score key.
        score: SortKey,
    },
    /// Row limit (per-fragment locally; global at the coordinator).
    DistributedLimit {
        /// Row limit.
        limit: usize,
    },
}

/// How rows move across one exchange edge (spec section 12.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeKind {
    /// Rows are hash-routed on `keys` across the sibling consumers of the
    /// producing fragment.
    HashRepartition {
        /// Hash key columns (in the producer's output schema).
        keys: Vec<String>,
    },
    /// The producer's full output is replicated to every consumer.
    Broadcast,
    /// The producer's output flows to its single consumer.
    Merge,
}

/// One exchange edge between two fragments (spec section 12.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeDescriptor {
    /// Plan-local exchange identifier.
    pub exchange_id: ExchangeId,
    /// Producing fragment.
    pub producer: FragmentId,
    /// Consuming fragment.
    pub consumer: FragmentId,
    /// Row movement kind.
    pub kind: ExchangeKind,
    /// FNV-1a fingerprint of the producer's output column names (types join
    /// the fingerprint with the DataFusion lowering wave).
    pub schema_fingerprint: u64,
}

/// One executable unit of a [`DistributedPlan`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanFragment {
    /// Plan-local fragment identifier.
    pub fragment_id: FragmentId,
    /// Where the fragment executes.
    pub assignment: FragmentAssignment,
    /// Operators, in execution order (scans/sources first, sink last).
    pub operators: Vec<FragmentOperator>,
    /// Estimated output rows (spec section 12.10).
    pub estimated_rows: u64,
    /// Estimated output bytes (spec section 12.10).
    pub estimated_bytes: u64,
    /// Maximum spill allowance in bytes (spec section 12.10).
    pub max_spill_bytes: u64,
}

/// A distributed query plan pinned to one control-plane metadata version
/// (spec section 12.10).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DistributedPlan {
    /// The query execution this plan belongs to.
    pub query_id: QueryId,
    /// The control-plane metadata version the plan was built against.
    pub metadata_version: MetadataVersion,
    /// All fragments; `fragment_id` equals the vector index.
    pub fragments: Vec<PlanFragment>,
    /// All exchange edges; `exchange_id` equals the vector index.
    pub exchanges: Vec<ExchangeDescriptor>,
}

impl DistributedPlan {
    /// The fragment with no outgoing exchange (the coordinator root).
    pub fn root_fragment_id(&self) -> Option<FragmentId> {
        self.fragments
            .iter()
            .map(|fragment| fragment.fragment_id)
            .find(|id| !self.exchanges.iter().any(|edge| edge.producer == *id))
    }

    /// Looks up a fragment by id.
    pub fn fragment(&self, fragment_id: FragmentId) -> Option<&PlanFragment> {
        self.fragments.get(fragment_id as usize)
    }

    /// Exchange edges out of one fragment.
    pub fn exchanges_from(
        &self,
        producer: FragmentId,
    ) -> impl Iterator<Item = &ExchangeDescriptor> {
        self.exchanges
            .iter()
            .filter(move |edge| edge.producer == producer)
    }

    /// Exchange edges into one fragment, ordered by producer id.
    pub fn exchanges_into(&self, consumer: FragmentId) -> Vec<&ExchangeDescriptor> {
        let mut edges: Vec<&ExchangeDescriptor> = self
            .exchanges
            .iter()
            .filter(|edge| edge.consumer == consumer)
            .collect();
        edges.sort_by_key(|edge| edge.producer);
        edges
    }
}

// ---------------------------------------------------------------------------
// Planner
// ---------------------------------------------------------------------------

/// Everything [`distribute`] needs to build a [`DistributedPlan`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanDescription {
    /// The query execution id the plan is pinned to.
    pub query_id: QueryId,
    /// The logical input tree.
    pub root: LogicalPlanLite,
    /// Planner tuning.
    pub options: PlannerOptions,
}

/// Planner tuning knobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerOptions {
    /// Small-side byte threshold below which a join is planned as broadcast.
    pub broadcast_threshold_bytes: u64,
    /// Per-fragment maximum spill allowance stamped on every fragment.
    pub max_spill_bytes_per_fragment: u64,
}

impl Default for PlannerOptions {
    fn default() -> Self {
        Self {
            broadcast_threshold_bytes: DEFAULT_BROADCAST_THRESHOLD_BYTES,
            max_spill_bytes_per_fragment: DEFAULT_MAX_SPILL_BYTES_PER_FRAGMENT,
        }
    }
}

/// Lowers a [`LogicalPlanLite`] tree onto tablets (spec section 12.10).
///
/// Join strategy, in decision order:
///
/// 1. **Colocated** — both inputs are scans whose partition specs are
///    colocated ([`PartitionSpec::colocated_with`]) over the identical tablet
///    list; the hash join runs fused inside each shared tablet's fragment.
/// 2. **Broadcast** — the smaller input's estimated bytes fit under
///    [`PlannerOptions::broadcast_threshold_bytes`]; the small side is
///    replicated to every big-side fragment.
/// 3. **Repartition** — otherwise; both sides are hash-repartitioned on their
///    join keys into fresh join fragments.
///
/// `Sort` with a single descending key and a limit plans as
/// [`FragmentOperator::DistributedTopK`]; any other sort plans as
/// [`FragmentOperator::MergeSort`]. Every fragment carries estimated
/// rows/bytes and the configured maximum spill allowance.
pub fn distribute(
    description: &PlanDescription,
    locator: &dyn TabletLocator,
    metadata: &dyn ClusterMetadata,
) -> DistributedResult<DistributedPlan> {
    validate_lite(&description.root)?;
    let mut planner = Planner {
        locator,
        metadata,
        options: description.options,
        fragments: Vec::new(),
        exchanges: Vec::new(),
    };
    let stage = planner.plan_node(&description.root)?;
    let root = match stage.producers.as_slice() {
        [only]
            if planner.fragments[*only as usize].assignment == FragmentAssignment::Coordinator =>
        {
            *only
        }
        _ => {
            let gather = planner.push_fragment(
                FragmentAssignment::Coordinator,
                Vec::new(),
                stage.estimated_rows,
                stage.estimated_bytes,
            );
            planner.wire(&stage.producers, &[gather], ExchangeKind::Merge);
            gather
        }
    };
    debug_assert_eq!(
        planner
            .fragments
            .iter()
            .filter(|fragment| {
                !planner
                    .exchanges
                    .iter()
                    .any(|edge| edge.producer == fragment.fragment_id)
            })
            .count(),
        1,
        "exactly one root fragment"
    );
    let _ = root;
    Ok(DistributedPlan {
        query_id: description.query_id,
        metadata_version: metadata.metadata_version(),
        fragments: planner.fragments,
        exchanges: planner.exchanges,
    })
}

/// Validates the input IR before planning.
fn validate_lite(node: &LogicalPlanLite) -> DistributedResult<()> {
    match node {
        LogicalPlanLite::Scan { table, .. } => {
            if table.is_empty() {
                return Err(DistributedError::InvalidPlan(
                    "scan needs a table name".to_owned(),
                ));
            }
        }
        LogicalPlanLite::Aggregate {
            input, aggregates, ..
        } => {
            for aggregate in aggregates {
                if aggregate.function != AggregateFunction::Count && aggregate.column.is_none() {
                    return Err(DistributedError::InvalidPlan(format!(
                        "{} needs a column",
                        aggregate.function.name()
                    )));
                }
            }
            validate_lite(input)?;
        }
        LogicalPlanLite::Join { left, right, on } => {
            if on.is_empty() {
                return Err(DistributedError::InvalidPlan(
                    "join needs at least one key".to_owned(),
                ));
            }
            validate_lite(left)?;
            validate_lite(right)?;
        }
        LogicalPlanLite::Sort { input, keys, .. } => {
            if keys.is_empty() {
                return Err(DistributedError::InvalidPlan(
                    "sort needs at least one key".to_owned(),
                ));
            }
            validate_lite(input)?;
        }
        LogicalPlanLite::Limit { input, .. } => validate_lite(input)?,
        LogicalPlanLite::Union { inputs } => {
            if inputs.len() < 2 {
                return Err(DistributedError::InvalidPlan(
                    "union needs at least two inputs".to_owned(),
                ));
            }
            for input in inputs {
                validate_lite(input)?;
            }
        }
    }
    Ok(())
}

/// The planner's view of one planned subtree's output stage.
struct Stage {
    /// Fragments producing this stage's output (sinks not yet attached).
    producers: Vec<FragmentId>,
    estimated_rows: u64,
    estimated_bytes: u64,
    /// Output partitioning, when known (scan stages and colocated joins).
    partitioning: Option<PartitionSpec>,
    /// Tablets the stage's fragments run on (empty for coordinator stages).
    tablets: Vec<TabletId>,
}

struct Planner<'a> {
    locator: &'a dyn TabletLocator,
    metadata: &'a dyn ClusterMetadata,
    options: PlannerOptions,
    fragments: Vec<PlanFragment>,
    exchanges: Vec<ExchangeDescriptor>,
}

impl Planner<'_> {
    fn push_fragment(
        &mut self,
        assignment: FragmentAssignment,
        operators: Vec<FragmentOperator>,
        estimated_rows: u64,
        estimated_bytes: u64,
    ) -> FragmentId {
        let fragment_id = self.fragments.len() as FragmentId;
        self.fragments.push(PlanFragment {
            fragment_id,
            assignment,
            operators,
            estimated_rows,
            estimated_bytes,
            max_spill_bytes: self.options.max_spill_bytes_per_fragment,
        });
        fragment_id
    }

    /// Connects every producer to every consumer with fresh exchange edges,
    /// appending the sink operators to producers and source operators to
    /// consumers (consumers must not have transform operators yet).
    fn wire(&mut self, producers: &[FragmentId], consumers: &[FragmentId], kind: ExchangeKind) {
        for &producer in producers {
            let schema_fingerprint = self.schema_fingerprint(producer);
            for &consumer in consumers {
                let exchange_id = self.exchanges.len() as ExchangeId;
                self.exchanges.push(ExchangeDescriptor {
                    exchange_id,
                    producer,
                    consumer,
                    kind: kind.clone(),
                    schema_fingerprint,
                });
                self.fragments[producer as usize].operators.push(
                    FragmentOperator::RemoteExchangeSink {
                        exchange: exchange_id,
                    },
                );
                self.fragments[consumer as usize].operators.push(
                    FragmentOperator::RemoteExchangeSource {
                        exchange: exchange_id,
                    },
                );
            }
        }
    }

    /// FNV-1a over the producer fragment's output column names.
    fn schema_fingerprint(&self, fragment_id: FragmentId) -> u64 {
        let columns = fragment_output_columns(&self.fragments[fragment_id as usize]);
        let mut bytes = Vec::new();
        for column in &columns {
            bytes.extend_from_slice(&(column.len() as u32).to_le_bytes());
            bytes.extend_from_slice(column.as_bytes());
        }
        fnv1a64(&bytes)
    }

    fn metadata_stats(&self, table: &str) -> DistributedResult<TableStats> {
        self.metadata.table_stats(table)
    }

    fn plan_node(&mut self, node: &LogicalPlanLite) -> DistributedResult<Stage> {
        match node {
            LogicalPlanLite::Scan {
                table,
                predicate,
                projection,
            } => self.plan_scan(table, predicate, projection),
            LogicalPlanLite::Aggregate {
                input,
                group_by,
                aggregates,
            } => self.plan_aggregate(input, group_by, aggregates),
            LogicalPlanLite::Join { left, right, on } => self.plan_join(left, right, on),
            LogicalPlanLite::Sort { input, keys, limit } => self.plan_sort(input, keys, *limit),
            LogicalPlanLite::Limit { input, limit } => self.plan_limit(input, *limit),
            LogicalPlanLite::Union { inputs } => self.plan_union(inputs),
        }
    }

    fn plan_scan(
        &mut self,
        table: &str,
        predicate: &Option<String>,
        projection: &[String],
    ) -> DistributedResult<Stage> {
        let tablets = self.locator.tablets_for_table(table)?;
        if tablets.is_empty() {
            return Err(DistributedError::EmptyLayout {
                table: table.to_owned(),
            });
        }
        let spec = self.locator.partitioning(table)?;
        let stats = self.metadata_stats(table)?;
        let count = tablets.len() as u64;
        let per_rows = stats.row_count.div_ceil(count);
        let per_bytes = stats.total_bytes.div_ceil(count);
        let mut producers = Vec::with_capacity(tablets.len());
        for tablet in &tablets {
            producers.push(self.push_fragment(
                FragmentAssignment::Tablet(*tablet),
                vec![FragmentOperator::TabletScan {
                    table: table.to_owned(),
                    predicate: predicate.clone(),
                    projection: projection.to_vec(),
                }],
                per_rows,
                per_bytes,
            ));
        }
        Ok(Stage {
            producers,
            estimated_rows: stats.row_count,
            estimated_bytes: stats.total_bytes,
            partitioning: Some(spec),
            tablets,
        })
    }

    fn plan_aggregate(
        &mut self,
        input: &LogicalPlanLite,
        group_by: &[String],
        aggregates: &[AggregateExpr],
    ) -> DistributedResult<Stage> {
        let child = self.plan_node(input)?;
        let estimated_rows = if group_by.is_empty() {
            1
        } else {
            (child.estimated_rows / 2).max(1)
        };
        let per_row = child
            .estimated_bytes
            .checked_div(child.estimated_rows.max(1))
            .unwrap_or(0)
            .max(1);
        let estimated_bytes = estimated_rows.saturating_mul(per_row);
        if let [only] = child.producers.as_slice() {
            if self.fragments[*only as usize].assignment == FragmentAssignment::Coordinator {
                // Single coordinator producer: combine in place, no shuffle.
                self.fragments[*only as usize].operators.extend([
                    FragmentOperator::PartialAggregate {
                        group_by: group_by.to_vec(),
                        aggregates: aggregates.to_vec(),
                    },
                    FragmentOperator::FinalAggregate {
                        group_by: group_by.to_vec(),
                        aggregates: aggregates.to_vec(),
                    },
                ]);
                return Ok(Stage {
                    estimated_rows,
                    estimated_bytes,
                    ..child
                });
            }
        }
        for &producer in &child.producers {
            self.fragments[producer as usize]
                .operators
                .push(FragmentOperator::PartialAggregate {
                    group_by: group_by.to_vec(),
                    aggregates: aggregates.to_vec(),
                });
        }
        let consumer = self.push_fragment(
            FragmentAssignment::Coordinator,
            Vec::new(),
            estimated_rows,
            estimated_bytes,
        );
        let kind = if group_by.is_empty() {
            ExchangeKind::Merge
        } else {
            ExchangeKind::HashRepartition {
                keys: group_by.to_vec(),
            }
        };
        self.wire(&child.producers, &[consumer], kind);
        self.fragments[consumer as usize]
            .operators
            .push(FragmentOperator::FinalAggregate {
                group_by: group_by.to_vec(),
                aggregates: aggregates.to_vec(),
            });
        Ok(Stage {
            producers: vec![consumer],
            estimated_rows,
            estimated_bytes,
            partitioning: None,
            tablets: Vec::new(),
        })
    }

    fn plan_join(
        &mut self,
        left: &LogicalPlanLite,
        right: &LogicalPlanLite,
        on: &[JoinKey],
    ) -> DistributedResult<Stage> {
        // Colocation can be decided from metadata alone (no fragments built).
        if let (
            LogicalPlanLite::Scan {
                table: left_table, ..
            },
            LogicalPlanLite::Scan {
                table: right_table, ..
            },
        ) = (left, right)
        {
            let left_tablets = self.locator.tablets_for_table(left_table)?;
            if left_tablets.is_empty() {
                return Err(DistributedError::EmptyLayout {
                    table: left_table.clone(),
                });
            }
            let right_tablets = self.locator.tablets_for_table(right_table)?;
            if right_tablets.is_empty() {
                return Err(DistributedError::EmptyLayout {
                    table: right_table.clone(),
                });
            }
            let left_spec = self.locator.partitioning(left_table)?;
            let right_spec = self.locator.partitioning(right_table)?;
            if left_spec.colocated_with(&right_spec) && left_tablets == right_tablets {
                return self.plan_colocated_join(left, right, on, &left_tablets, &left_spec);
            }
        }
        let left_stage = self.plan_node(left)?;
        let right_stage = self.plan_node(right)?;
        if left_stage.estimated_bytes.min(right_stage.estimated_bytes)
            <= self.options.broadcast_threshold_bytes
        {
            self.plan_broadcast_join(left_stage, right_stage, on)
        } else {
            self.plan_repartition_join(left_stage, right_stage, on)
        }
    }

    /// Fuses two colocated scans and the hash join into one fragment per
    /// shared tablet — no exchange at all (spec section 12.10).
    fn plan_colocated_join(
        &mut self,
        left: &LogicalPlanLite,
        right: &LogicalPlanLite,
        on: &[JoinKey],
        tablets: &[TabletId],
        spec: &PartitionSpec,
    ) -> DistributedResult<Stage> {
        let (
            LogicalPlanLite::Scan {
                table: left_table,
                predicate: left_predicate,
                projection: left_projection,
            },
            LogicalPlanLite::Scan {
                table: right_table,
                predicate: right_predicate,
                projection: right_projection,
            },
        ) = (left, right)
        else {
            return Err(DistributedError::InvalidPlan(
                "colocated join needs scan inputs".to_owned(),
            ));
        };
        let left_stats = self.metadata_stats(left_table)?;
        let right_stats = self.metadata_stats(right_table)?;
        let estimated_rows = left_stats.row_count.max(right_stats.row_count);
        let estimated_bytes = left_stats.total_bytes.max(right_stats.total_bytes);
        let count = tablets.len() as u64;
        let mut producers = Vec::with_capacity(tablets.len());
        for tablet in tablets {
            producers.push(self.push_fragment(
                FragmentAssignment::Tablet(*tablet),
                vec![
                    FragmentOperator::TabletScan {
                        table: left_table.clone(),
                        predicate: left_predicate.clone(),
                        projection: left_projection.clone(),
                    },
                    FragmentOperator::TabletScan {
                        table: right_table.clone(),
                        predicate: right_predicate.clone(),
                        projection: right_projection.clone(),
                    },
                    FragmentOperator::DistributedHashJoin { on: on.to_vec() },
                ],
                estimated_rows.div_ceil(count),
                estimated_bytes.div_ceil(count),
            ));
        }
        Ok(Stage {
            producers,
            estimated_rows,
            estimated_bytes,
            partitioning: Some(spec.clone()),
            tablets: tablets.to_vec(),
        })
    }

    /// Replicates the small side to every big-side fragment (spec section
    /// 12.10: "broadcast small side").
    fn plan_broadcast_join(
        &mut self,
        left_stage: Stage,
        right_stage: Stage,
        on: &[JoinKey],
    ) -> DistributedResult<Stage> {
        let (big, small, build_side) = if right_stage.estimated_bytes <= left_stage.estimated_bytes
        {
            (left_stage, right_stage, BuildSide::Right)
        } else {
            (right_stage, left_stage, BuildSide::Left)
        };
        self.wire(&small.producers, &big.producers, ExchangeKind::Broadcast);
        for &producer in &big.producers {
            self.fragments[producer as usize]
                .operators
                .push(FragmentOperator::BroadcastJoin {
                    on: on.to_vec(),
                    build_side,
                });
        }
        let estimated_rows = big.estimated_rows;
        let estimated_bytes = big.estimated_bytes.saturating_add(small.estimated_bytes);
        Ok(Stage {
            producers: big.producers,
            estimated_rows,
            estimated_bytes,
            partitioning: big.partitioning,
            tablets: big.tablets,
        })
    }

    /// Hash-repartitions both inputs on their join keys into fresh join
    /// fragments (spec section 12.10: "repartition both sides").
    fn plan_repartition_join(
        &mut self,
        left_stage: Stage,
        right_stage: Stage,
        on: &[JoinKey],
    ) -> DistributedResult<Stage> {
        let join_tablets = if left_stage.tablets.len() >= right_stage.tablets.len() {
            left_stage.tablets.clone()
        } else {
            right_stage.tablets.clone()
        };
        if join_tablets.is_empty() {
            return Err(DistributedError::InvalidPlan(
                "repartition join needs at least one tablet-backed input".to_owned(),
            ));
        }
        let width = left_stage.producers.len().max(right_stage.producers.len());
        let estimated_rows = left_stage.estimated_rows.max(right_stage.estimated_rows);
        let estimated_bytes = left_stage
            .estimated_bytes
            .saturating_add(right_stage.estimated_bytes);
        let mut consumers = Vec::with_capacity(width);
        for index in 0..width {
            consumers.push(self.push_fragment(
                FragmentAssignment::Tablet(join_tablets[index % join_tablets.len()]),
                Vec::new(),
                estimated_rows.div_ceil(width as u64),
                estimated_bytes.div_ceil(width as u64),
            ));
        }
        let left_keys: Vec<String> = on.iter().map(|key| key.left.clone()).collect();
        let right_keys: Vec<String> = on.iter().map(|key| key.right.clone()).collect();
        self.wire(
            &left_stage.producers,
            &consumers,
            ExchangeKind::HashRepartition { keys: left_keys },
        );
        self.wire(
            &right_stage.producers,
            &consumers,
            ExchangeKind::HashRepartition { keys: right_keys },
        );
        for &consumer in &consumers {
            self.fragments[consumer as usize]
                .operators
                .push(FragmentOperator::RepartitionJoin { on: on.to_vec() });
        }
        Ok(Stage {
            producers: consumers,
            estimated_rows,
            estimated_bytes,
            partitioning: None,
            tablets: join_tablets,
        })
    }

    fn plan_sort(
        &mut self,
        input: &LogicalPlanLite,
        keys: &[SortKey],
        limit: Option<usize>,
    ) -> DistributedResult<Stage> {
        let child = self.plan_node(input)?;
        // The distributed top-k shape (spec section 12.10): one descending
        // score key plus a limit. Anything else is a merge sort.
        let top_k = match (limit, keys) {
            (Some(k), [score]) if score.descending => Some((k, score.clone())),
            _ => None,
        };
        let estimated_rows = limit.map_or(child.estimated_rows, |limit| {
            child.estimated_rows.min(limit as u64)
        });
        let estimated_bytes =
            scaled_bytes(child.estimated_bytes, child.estimated_rows, estimated_rows);
        let local_op = match &top_k {
            Some((k, score)) => FragmentOperator::DistributedTopK {
                k: *k,
                score: score.clone(),
            },
            None => FragmentOperator::MergeSort {
                keys: keys.to_vec(),
                limit,
            },
        };
        if let [only] = child.producers.as_slice() {
            if self.fragments[*only as usize].assignment == FragmentAssignment::Coordinator {
                self.fragments[*only as usize].operators.push(local_op);
                return Ok(Stage {
                    estimated_rows,
                    estimated_bytes,
                    ..child
                });
            }
        }
        for &producer in &child.producers {
            self.fragments[producer as usize]
                .operators
                .push(local_op.clone());
        }
        let consumer = self.push_fragment(
            FragmentAssignment::Coordinator,
            Vec::new(),
            estimated_rows,
            estimated_bytes,
        );
        self.wire(&child.producers, &[consumer], ExchangeKind::Merge);
        let root_op = match &top_k {
            Some((k, score)) => FragmentOperator::DistributedTopK {
                k: *k,
                score: score.clone(),
            },
            None => FragmentOperator::MergeSort {
                keys: keys.to_vec(),
                limit,
            },
        };
        self.fragments[consumer as usize].operators.push(root_op);
        Ok(Stage {
            producers: vec![consumer],
            estimated_rows,
            estimated_bytes,
            partitioning: None,
            tablets: Vec::new(),
        })
    }

    fn plan_limit(&mut self, input: &LogicalPlanLite, limit: usize) -> DistributedResult<Stage> {
        let child = self.plan_node(input)?;
        let estimated_rows = child.estimated_rows.min(limit as u64);
        let estimated_bytes =
            scaled_bytes(child.estimated_bytes, child.estimated_rows, estimated_rows);
        if let [only] = child.producers.as_slice() {
            if self.fragments[*only as usize].assignment == FragmentAssignment::Coordinator {
                self.fragments[*only as usize]
                    .operators
                    .push(FragmentOperator::DistributedLimit { limit });
                return Ok(Stage {
                    estimated_rows,
                    estimated_bytes,
                    ..child
                });
            }
        }
        for &producer in &child.producers {
            self.fragments[producer as usize]
                .operators
                .push(FragmentOperator::DistributedLimit { limit });
        }
        let consumer = self.push_fragment(
            FragmentAssignment::Coordinator,
            Vec::new(),
            estimated_rows,
            estimated_bytes,
        );
        self.wire(&child.producers, &[consumer], ExchangeKind::Merge);
        self.fragments[consumer as usize]
            .operators
            .push(FragmentOperator::DistributedLimit { limit });
        Ok(Stage {
            producers: vec![consumer],
            estimated_rows,
            estimated_bytes,
            partitioning: None,
            tablets: Vec::new(),
        })
    }

    fn plan_union(&mut self, inputs: &[LogicalPlanLite]) -> DistributedResult<Stage> {
        let mut producers = Vec::new();
        let mut estimated_rows = 0u64;
        let mut estimated_bytes = 0u64;
        for input in inputs {
            let stage = self.plan_node(input)?;
            producers.extend(stage.producers);
            estimated_rows = estimated_rows.saturating_add(stage.estimated_rows);
            estimated_bytes = estimated_bytes.saturating_add(stage.estimated_bytes);
        }
        if producers.is_empty() {
            return Err(DistributedError::InvalidPlan(
                "union produced no fragments".to_owned(),
            ));
        }
        // All inputs already coordinator-local and single-producer: keep them
        // as-is (no extra gather hop).
        if producers.len() == 1 {
            return Ok(Stage {
                producers,
                estimated_rows,
                estimated_bytes,
                partitioning: None,
                tablets: Vec::new(),
            });
        }
        let consumer = self.push_fragment(
            FragmentAssignment::Coordinator,
            Vec::new(),
            estimated_rows,
            estimated_bytes,
        );
        self.wire(&producers, &[consumer], ExchangeKind::Merge);
        Ok(Stage {
            producers: vec![consumer],
            estimated_rows,
            estimated_bytes,
            partitioning: None,
            tablets: Vec::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// DataFusion → DistributedPlan lowering (P0.4)
// ---------------------------------------------------------------------------

/// Lowers a DataFusion [`datafusion::logical_expr::LogicalPlan`] onto tablets.
///
/// Conversion is two-phase: the DataFusion tree is reduced to
/// [`LogicalPlanLite`], then [`distribute`] places fragments. Filters and
/// projections are pushed onto base scans; residual filters that cannot be
/// pushed are rejected rather than stringified without worker evaluation.
#[derive(Clone, Debug)]
pub struct DataFusionDistributedPlanner {
    query_id: QueryId,
    options: PlannerOptions,
}

impl DataFusionDistributedPlanner {
    /// Planner pinned to one query id with default options.
    pub fn new(query_id: QueryId) -> Self {
        Self {
            query_id,
            options: PlannerOptions::default(),
        }
    }

    /// Planner with explicit fragment/join tuning.
    pub fn with_options(query_id: QueryId, options: PlannerOptions) -> Self {
        Self { query_id, options }
    }

    /// Query id this planner stamps onto every plan.
    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    /// Lowers `plan` into a [`DistributedPlan`] against the given locator and
    /// control-plane metadata.
    pub fn lower(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
        locator: &dyn TabletLocator,
        metadata: &dyn ClusterMetadata,
    ) -> DistributedResult<DistributedPlan> {
        let root = self.to_lite(plan)?;
        distribute(
            &PlanDescription {
                query_id: self.query_id,
                root,
                options: self.options,
            },
            locator,
            metadata,
        )
    }

    /// Converts a DataFusion logical plan into the distributed IR without
    /// tablet placement. Useful for unit tests and diagnostics.
    pub fn to_lite(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
    ) -> DistributedResult<LogicalPlanLite> {
        lower_datafusion_plan(plan)
    }
}

fn lower_datafusion_plan(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> DistributedResult<LogicalPlanLite> {
    use datafusion::logical_expr::LogicalPlan;

    match plan {
        LogicalPlan::TableScan(scan) => lower_table_scan(scan),
        LogicalPlan::Projection(projection) => lower_projection(projection),
        LogicalPlan::Filter(filter) => lower_filter(filter),
        LogicalPlan::Aggregate(aggregate) => lower_aggregate(aggregate),
        LogicalPlan::Sort(sort) => lower_sort(sort),
        LogicalPlan::Limit(limit) => lower_limit(limit),
        LogicalPlan::Join(join) => lower_join(join),
        LogicalPlan::Union(union) => lower_union(union),
        LogicalPlan::SubqueryAlias(alias) => lower_datafusion_plan(alias.input.as_ref()),
        other => Err(DistributedError::Unsupported(format!(
            "DataFusion operator not supported for distributed planning: {}",
            other.display()
        ))),
    }
}

fn lower_table_scan(
    scan: &datafusion::logical_expr::TableScan,
) -> DistributedResult<LogicalPlanLite> {
    let table = scan.table_name.table().to_owned();
    if table.is_empty() {
        return Err(DistributedError::InvalidPlan(
            "table scan needs a table name".to_owned(),
        ));
    }
    let projection = if let Some(indices) = &scan.projection {
        let schema = scan.source.schema();
        let mut columns = Vec::with_capacity(indices.len());
        for &idx in indices {
            if idx >= schema.fields().len() {
                return Err(DistributedError::InvalidPlan(format!(
                    "table scan projection index {idx} out of range"
                )));
            }
            columns.push(schema.field(idx).name().clone());
        }
        columns
    } else {
        // projected_schema already reflects provider projection when present.
        scan.projected_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    };
    let predicate = combine_filter_predicates(&scan.filters)?;
    let mut root = LogicalPlanLite::Scan {
        table,
        predicate,
        projection,
    };
    if let Some(fetch) = scan.fetch {
        root = LogicalPlanLite::Limit {
            input: Box::new(root),
            limit: fetch,
        };
    }
    Ok(root)
}

fn lower_projection(
    projection: &datafusion::logical_expr::Projection,
) -> DistributedResult<LogicalPlanLite> {
    let mut input = lower_datafusion_plan(projection.input.as_ref())?;
    let columns = projection_column_names(&projection.expr)?;
    // Pure column projections fold into the base scan when possible.
    if let Some((predicate, projection_cols)) = scan_fields_mut(&mut input) {
        let _ = predicate; // projection does not touch the predicate
        if !columns.is_empty() {
            if projection_cols.is_empty() {
                *projection_cols = columns;
            } else {
                // Intersect/reorder existing projection by requested names.
                let map: HashMap<String, String> = projection_cols
                    .iter()
                    .map(|name| (name.clone(), name.clone()))
                    .collect();
                let mut next = Vec::with_capacity(columns.len());
                for column in &columns {
                    let Some(existing) = map.get(column.as_str()) else {
                        return Err(DistributedError::InvalidPlan(format!(
                            "projection column `{column}` is not in the scan projection"
                        )));
                    };
                    next.push(existing.clone());
                }
                *projection_cols = next;
            }
        }
        return Ok(input);
    }
    // Non-scan inputs (Aggregate / Sort / Limit / Join / Union): treat pure
    // column renames as identity. The fragment model has no free-standing
    // Projection operator; final output naming is coordinator-side.
    if columns.is_empty()
        || matches!(
            input,
            LogicalPlanLite::Aggregate { .. }
                | LogicalPlanLite::Sort { .. }
                | LogicalPlanLite::Limit { .. }
                | LogicalPlanLite::Join { .. }
                | LogicalPlanLite::Union { .. }
        )
    {
        return Ok(input);
    }
    Err(DistributedError::Unsupported(
        "projection over a non-scan plan is not supported; push the projection onto the base table"
            .to_owned(),
    ))
}

fn lower_filter(filter: &datafusion::logical_expr::Filter) -> DistributedResult<LogicalPlanLite> {
    // Require a worker-evaluable predicate shape before accepting it.
    assert_filter_evaluable(&filter.predicate)?;
    let mut input = lower_datafusion_plan(filter.input.as_ref())?;
    let text = filter.predicate.to_string();
    if let Some((predicate, _)) = scan_fields_mut(&mut input) {
        *predicate = match predicate.take() {
            Some(existing) => Some(format!("({existing}) AND ({text})")),
            None => Some(text),
        };
        return Ok(input);
    }
    Err(DistributedError::Unsupported(
        "filter must push down onto a table scan for distributed execution".to_owned(),
    ))
}

fn lower_aggregate(
    aggregate: &datafusion::logical_expr::Aggregate,
) -> DistributedResult<LogicalPlanLite> {
    let input = lower_datafusion_plan(aggregate.input.as_ref())?;
    let mut group_by = Vec::with_capacity(aggregate.group_expr.len());
    for expr in &aggregate.group_expr {
        group_by.push(column_name(expr).ok_or_else(|| {
            DistributedError::Unsupported(format!(
                "aggregate group-by expression must be a column, got {expr}"
            ))
        })?);
    }
    let mut aggregates = Vec::with_capacity(aggregate.aggr_expr.len());
    for expr in &aggregate.aggr_expr {
        aggregates.push(aggregate_expr_from_df(expr)?);
    }
    if aggregates.is_empty() {
        return Err(DistributedError::InvalidPlan(
            "aggregate needs at least one aggregate expression".to_owned(),
        ));
    }
    Ok(LogicalPlanLite::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates,
    })
}

fn lower_sort(sort: &datafusion::logical_expr::Sort) -> DistributedResult<LogicalPlanLite> {
    let input = lower_datafusion_plan(sort.input.as_ref())?;
    if sort.expr.is_empty() {
        return Err(DistributedError::InvalidPlan(
            "sort needs at least one key".to_owned(),
        ));
    }
    let mut keys = Vec::with_capacity(sort.expr.len());
    for sort_expr in &sort.expr {
        let column = column_name(&sort_expr.expr).ok_or_else(|| {
            DistributedError::Unsupported(format!(
                "sort key must be a column, got {}",
                sort_expr.expr
            ))
        })?;
        keys.push(SortKey {
            column,
            descending: !sort_expr.asc,
        });
    }
    Ok(LogicalPlanLite::Sort {
        input: Box::new(input),
        keys,
        limit: sort.fetch,
    })
}

fn lower_limit(limit: &datafusion::logical_expr::Limit) -> DistributedResult<LogicalPlanLite> {
    use datafusion::logical_expr::{FetchType, SkipType};

    match limit.get_skip_type() {
        Ok(SkipType::Literal(0)) => {}
        Ok(SkipType::Literal(skip)) => {
            return Err(DistributedError::Unsupported(format!(
                "OFFSET {skip} is not supported in distributed planning"
            )));
        }
        Ok(SkipType::UnsupportedExpr) => {
            return Err(DistributedError::Unsupported(
                "non-literal OFFSET is not supported in distributed planning".to_owned(),
            ));
        }
        Err(error) => {
            return Err(DistributedError::InvalidPlan(error.to_string()));
        }
    }
    let fetch = match limit.get_fetch_type() {
        Ok(FetchType::Literal(Some(n))) => n,
        Ok(FetchType::Literal(None)) => {
            // LIMIT with no fetch is a no-op.
            return lower_datafusion_plan(limit.input.as_ref());
        }
        Ok(FetchType::UnsupportedExpr) => {
            return Err(DistributedError::Unsupported(
                "non-literal LIMIT is not supported in distributed planning".to_owned(),
            ));
        }
        Err(error) => return Err(DistributedError::InvalidPlan(error.to_string())),
    };
    let input = lower_datafusion_plan(limit.input.as_ref())?;
    Ok(LogicalPlanLite::Limit {
        input: Box::new(input),
        limit: fetch,
    })
}

fn lower_join(join: &datafusion::logical_expr::Join) -> DistributedResult<LogicalPlanLite> {
    use datafusion::logical_expr::{JoinConstraint, JoinType};

    if join.join_type != JoinType::Inner {
        return Err(DistributedError::Unsupported(format!(
            "only Inner joins are supported for distributed planning, got {:?}",
            join.join_type
        )));
    }
    if join.filter.is_some() {
        return Err(DistributedError::Unsupported(
            "non-equi join filters are not supported for distributed planning".to_owned(),
        ));
    }
    if !matches!(
        join.join_constraint,
        JoinConstraint::On | JoinConstraint::Using
    ) {
        return Err(DistributedError::Unsupported(format!(
            "unsupported join constraint {:?}",
            join.join_constraint
        )));
    }
    if join.on.is_empty() {
        return Err(DistributedError::InvalidPlan(
            "join needs at least one key".to_owned(),
        ));
    }
    let left = lower_datafusion_plan(join.left.as_ref())?;
    let right = lower_datafusion_plan(join.right.as_ref())?;
    let mut on = Vec::with_capacity(join.on.len());
    for (left_expr, right_expr) in &join.on {
        let left_col = column_name(left_expr).ok_or_else(|| {
            DistributedError::Unsupported(format!(
                "join key must be a column, got left={left_expr}"
            ))
        })?;
        let right_col = column_name(right_expr).ok_or_else(|| {
            DistributedError::Unsupported(format!(
                "join key must be a column, got right={right_expr}"
            ))
        })?;
        on.push(JoinKey {
            left: left_col,
            right: right_col,
        });
    }
    Ok(LogicalPlanLite::Join {
        left: Box::new(left),
        right: Box::new(right),
        on,
    })
}

fn lower_union(union: &datafusion::logical_expr::Union) -> DistributedResult<LogicalPlanLite> {
    if union.inputs.len() < 2 {
        return Err(DistributedError::InvalidPlan(
            "union needs at least two inputs".to_owned(),
        ));
    }
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        inputs.push(lower_datafusion_plan(input.as_ref())?);
    }
    Ok(LogicalPlanLite::Union { inputs })
}

/// Mutable access to a base-scan's predicate and projection, including when the
/// scan is wrapped by a table-scan `fetch` limit.
fn scan_fields_mut(node: &mut LogicalPlanLite) -> Option<(&mut Option<String>, &mut Vec<String>)> {
    match node {
        LogicalPlanLite::Scan {
            predicate,
            projection,
            ..
        } => Some((predicate, projection)),
        LogicalPlanLite::Limit { input, .. } => match input.as_mut() {
            LogicalPlanLite::Scan {
                predicate,
                projection,
                ..
            } => Some((predicate, projection)),
            _ => None,
        },
        _ => None,
    }
}

fn projection_column_names(
    exprs: &[datafusion::logical_expr::Expr],
) -> DistributedResult<Vec<String>> {
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        if let Some(name) = column_name(expr) {
            columns.push(name);
            continue;
        }
        // Unresolved wildcards (or other non-column exprs) are rejected —
        // DataFusion should expand them before distributed lowering.
        return Err(DistributedError::Unsupported(format!(
            "projection expression must be a column (or alias of a column), got {expr}"
        )));
    }
    Ok(columns)
}

fn column_name(expr: &datafusion::logical_expr::Expr) -> Option<String> {
    use datafusion::logical_expr::Expr;
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Alias(alias) => column_name(alias.expr.as_ref()),
        _ => None,
    }
}

fn combine_filter_predicates(
    filters: &[datafusion::logical_expr::Expr],
) -> DistributedResult<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(filters.len());
    for filter in filters {
        assert_filter_evaluable(filter)?;
        parts.push(format!("({filter})"));
    }
    Ok(Some(parts.join(" AND ")))
}

/// Accept only predicates the distributed worker can evaluate without an
/// opaque, untyped blob. Column comparisons against literals (and boolean
/// combinations of those) are allowed; everything else is rejected.
fn assert_filter_evaluable(expr: &datafusion::logical_expr::Expr) -> DistributedResult<()> {
    use datafusion::logical_expr::{Expr, Operator};

    match expr {
        Expr::Column(_) | Expr::Literal(_, _) | Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(()),
        Expr::Alias(alias) => assert_filter_evaluable(alias.expr.as_ref()),
        Expr::Not(inner) => assert_filter_evaluable(inner.as_ref()),
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And
            | Operator::Or
            | Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                assert_filter_evaluable(binary.left.as_ref())?;
                assert_filter_evaluable(binary.right.as_ref())
            }
            other => Err(DistributedError::Unsupported(format!(
                "filter operator {other:?} is not supported for distributed planning"
            ))),
        },
        Expr::Between(between) => {
            assert_filter_evaluable(between.expr.as_ref())?;
            assert_filter_evaluable(between.low.as_ref())?;
            assert_filter_evaluable(between.high.as_ref())
        }
        Expr::InList(list) => {
            assert_filter_evaluable(list.expr.as_ref())?;
            for value in &list.list {
                assert_filter_evaluable(value)?;
            }
            Ok(())
        }
        Expr::Like(like) => {
            assert_filter_evaluable(like.expr.as_ref())?;
            assert_filter_evaluable(like.pattern.as_ref())
        }
        other => Err(DistributedError::Unsupported(format!(
            "filter expression is not supported for distributed planning: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Public SQL → DistributedPlan entry (P0.4 gateway seam)
// ---------------------------------------------------------------------------

/// Arrow schemas for tables that DataFusion must resolve while planning SQL
/// for distributed placement. Schemas are planning-only (no row data).
#[derive(Clone, Debug, Default)]
pub struct PlanningTableCatalog {
    tables: HashMap<String, SchemaRef>,
}

impl PlanningTableCatalog {
    /// Empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) one table's schema for SQL planning.
    pub fn insert(&mut self, table: impl Into<String>, schema: SchemaRef) {
        self.tables.insert(table.into(), schema);
    }

    /// Builder-style registration.
    pub fn with_table(mut self, table: impl Into<String>, schema: SchemaRef) -> Self {
        self.insert(table, schema);
        self
    }

    /// Registered table names (sorted for stable diagnostics).
    pub fn table_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.tables.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Look up one table schema.
    pub fn schema(&self, table: &str) -> Option<&SchemaRef> {
        self.tables.get(table)
    }
}

/// Lowers an already-parsed DataFusion logical plan onto tablets.
///
/// Prefer [`plan_sql_distributed`] when the caller only has SQL text. This
/// entry is useful when [`crate::MongrelSession`] (or another frontend) has
/// already produced a resolved `LogicalPlan`.
pub fn plan_logical_distributed(
    plan: &datafusion::logical_expr::LogicalPlan,
    locator: &dyn TabletLocator,
    metadata: &dyn ClusterMetadata,
) -> DistributedResult<DistributedPlan> {
    plan_logical_distributed_with_id(plan, QueryId::new_random(), locator, metadata)
}

/// Same as [`plan_logical_distributed`] but stamps a caller-chosen query id.
pub fn plan_logical_distributed_with_id(
    plan: &datafusion::logical_expr::LogicalPlan,
    query_id: QueryId,
    locator: &dyn TabletLocator,
    metadata: &dyn ClusterMetadata,
) -> DistributedResult<DistributedPlan> {
    DataFusionDistributedPlanner::new(query_id).lower(plan, locator, metadata)
}

/// Public gateway entry: parse `sql` with DataFusion against `catalog`, then
/// lower via [`DataFusionDistributedPlanner`] onto `locator`/`metadata`.
///
/// This is the product seam for cluster SQL planning (P0.4). Callers supply
/// planning schemas (column names/types) for every table the SQL references;
/// placement uses [`TabletLocator`] / [`ClusterMetadata`], not a standalone
/// local catalog scan.
pub async fn plan_sql_distributed(
    sql: &str,
    catalog: &PlanningTableCatalog,
    locator: &dyn TabletLocator,
    metadata: &dyn ClusterMetadata,
) -> DistributedResult<DistributedPlan> {
    plan_sql_distributed_with_id(sql, catalog, QueryId::new_random(), locator, metadata).await
}

/// Same as [`plan_sql_distributed`] but stamps a caller-chosen query id.
pub async fn plan_sql_distributed_with_id(
    sql: &str,
    catalog: &PlanningTableCatalog,
    query_id: QueryId,
    locator: &dyn TabletLocator,
    metadata: &dyn ClusterMetadata,
) -> DistributedResult<DistributedPlan> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(DistributedError::InvalidPlan(
            "SQL statement is empty".to_owned(),
        ));
    }
    let ctx = datafusion::prelude::SessionContext::new();
    for (name, schema) in &catalog.tables {
        // Empty MemTable: planning needs schema only; workers scan real tablets.
        let provider = datafusion::datasource::MemTable::try_new(
            Arc::clone(schema),
            vec![vec![RecordBatch::new_empty(Arc::clone(schema))]],
        )
        .map_err(|error| {
            DistributedError::InvalidPlan(format!(
                "failed to register planning table `{name}`: {error}"
            ))
        })?;
        ctx.register_table(name.as_str(), Arc::new(provider))
            .map_err(|error| {
                DistributedError::InvalidPlan(format!(
                    "failed to register planning table `{name}`: {error}"
                ))
            })?;
    }
    let df = ctx.sql(sql).await.map_err(|error| {
        DistributedError::InvalidPlan(format!("DataFusion failed to plan SQL: {error}"))
    })?;
    plan_logical_distributed_with_id(df.logical_plan(), query_id, locator, metadata)
}

fn aggregate_expr_from_df(
    expr: &datafusion::logical_expr::Expr,
) -> DistributedResult<AggregateExpr> {
    use datafusion::logical_expr::Expr;

    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(agg) = expr else {
        return Err(DistributedError::Unsupported(format!(
            "aggregate expression must be an aggregate function, got {expr}"
        )));
    };
    let name = agg.func.name().to_ascii_lowercase();
    let function = match name.as_str() {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        "avg" | "mean" => AggregateFunction::Avg,
        other => {
            return Err(DistributedError::Unsupported(format!(
                "aggregate function `{other}` is not supported for distributed planning"
            )));
        }
    };
    let column = match agg.params.args.as_slice() {
        [] => None,
        // COUNT(*) / COUNT(1) have no column; COUNT(col) keeps the column.
        [arg] if function == AggregateFunction::Count => column_name(arg),
        [arg] => Some(column_name(arg).ok_or_else(|| {
            DistributedError::Unsupported(format!("aggregate argument must be a column, got {arg}"))
        })?),
        _ => {
            return Err(DistributedError::Unsupported(
                "multi-argument aggregates are not supported for distributed planning".to_owned(),
            ));
        }
    };
    if function != AggregateFunction::Count && column.is_none() {
        return Err(DistributedError::InvalidPlan(format!(
            "{} needs a column",
            function.name()
        )));
    }
    Ok(AggregateExpr { function, column })
}

/// Scales a byte estimate to a new row estimate.
fn scaled_bytes(bytes: u64, rows: u64, new_rows: u64) -> u64 {
    if rows == 0 {
        return 0;
    }
    bytes
        .saturating_mul(new_rows)
        .checked_div(rows)
        .unwrap_or(bytes)
}

/// Derives a fragment's output column names (fingerprint input).
fn fragment_output_columns(fragment: &PlanFragment) -> Vec<String> {
    let mut columns = Vec::new();
    for operator in &fragment.operators {
        match operator {
            FragmentOperator::TabletScan {
                table, projection, ..
            } => {
                if projection.is_empty() {
                    columns = vec![format!("{table}.*")];
                } else {
                    columns = projection
                        .iter()
                        .map(|column| format!("{table}.{column}"))
                        .collect();
                }
            }
            FragmentOperator::PartialAggregate {
                group_by,
                aggregates,
            } => {
                columns = group_by.to_vec();
                columns.extend(partial_column_names(aggregates));
            }
            FragmentOperator::FinalAggregate {
                group_by,
                aggregates,
            } => {
                columns = group_by.to_vec();
                columns.extend(aggregates.iter().map(aggregate_output_name));
            }
            FragmentOperator::DistributedHashJoin { .. }
            | FragmentOperator::BroadcastJoin { .. }
            | FragmentOperator::RepartitionJoin { .. } => {
                columns = vec!["*join*".to_owned()];
            }
            FragmentOperator::RemoteExchangeSource { .. }
            | FragmentOperator::RemoteExchangeSink { .. }
            | FragmentOperator::MergeSort { .. }
            | FragmentOperator::DistributedTopK { .. }
            | FragmentOperator::DistributedLimit { .. } => {}
        }
    }
    columns
}

/// Partial-aggregate output column names (`__partial_0`, and `__partial_i_sum`
/// / `__partial_i_count` for averages).
fn partial_column_names(aggregates: &[AggregateExpr]) -> Vec<String> {
    let mut names = Vec::new();
    for (index, aggregate) in aggregates.iter().enumerate() {
        if aggregate.function == AggregateFunction::Avg {
            names.push(format!("__partial_{index}_sum"));
            names.push(format!("__partial_{index}_count"));
        } else {
            names.push(format!("__partial_{index}"));
        }
    }
    names
}

/// Final-aggregate output column name (`count_star`, `sum_cost`, ...).
fn aggregate_output_name(aggregate: &AggregateExpr) -> String {
    format!(
        "{}_{}",
        aggregate.function.name(),
        aggregate.column.as_deref().unwrap_or("star")
    )
}

// ---------------------------------------------------------------------------
// Distributed top-k (spec section 12.10): deterministic merge + adaptive refill
// ---------------------------------------------------------------------------

/// One ranked row in the top-k model: the score plus the exact tie-break
/// identity (spec section 12.10: final score descending, tablet id ascending,
/// [`RowId`] ascending).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopKCandidate {
    /// Score mapped onto an order-preserving `u64` key (higher wins).
    pub score: u64,
    /// The tablet that contributed the row.
    pub tablet: TabletId,
    /// The row's id within the table.
    pub row_id: RowId,
}

/// The deterministic winner order: score descending, then tablet id
/// ascending, then [`RowId`] ascending. Returns [`Ordering::Less`] when `a`
/// ranks strictly better than `b`.
pub fn topk_cmp(a: &TopKCandidate, b: &TopKCandidate) -> Ordering {
    b.score
        .cmp(&a.score)
        .then_with(|| a.tablet.cmp(&b.tablet))
        .then_with(|| a.row_id.cmp(&b.row_id))
}

/// One tablet's bounded local top-k plus tie information (spec section
/// 12.10: "each tablet returns a bounded local top-k plus tie information").
#[derive(Clone, Debug)]
pub struct TabletTopK {
    /// The contributing tablet.
    pub tablet: TabletId,
    /// Local winners, best first (at most the bounded size).
    pub rows: Vec<TopKCandidate>,
    /// Upper bound on the score of any row NOT in `rows` (`None` = the tablet
    /// is exhausted — every local row has been returned).
    pub unseen_bound: Option<u64>,
}

/// The outcome of one deterministic coordinator merge step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopKMerge {
    /// The best `k` received candidates, best first.
    pub winners: Vec<TopKCandidate>,
    /// Tablets that must be refilled before `winners` is provably the exact
    /// global top-k (empty = the result is exact).
    pub refill: Vec<TabletId>,
}

/// Deterministically merges bounded local top-ks (spec section 12.10:
/// "coordinator merges deterministically").
///
/// Winners are the best `k` received candidates under [`topk_cmp`]. A tablet
/// lands in [`TopKMerge::refill`] when its unseen rows could still displace a
/// winner — i.e. when fewer than `k` candidates were received in total and
/// the tablet is not exhausted, or when the tablet's most optimistic unseen
/// candidate (score = `unseen_bound`, smallest possible [`RowId`]) ranks at
/// least as well as the current `k`-th winner. The tie case is deliberately
/// conservative (unseen row ids are unknown), so a refill may be requested
/// that returns no winning rows; correctness never depends on it.
///
/// Exactness invariant: when `refill` is empty, `winners` equals the global
/// top-`k` of all rows on all tablets. Proof sketch: any unseen row of tablet
/// `t` ranks no better than the optimistic candidate `(unseen_bound, t,
/// RowId::MIN)` — its score is at most the bound, and at equal score its row
/// id is larger — so when the optimistic candidate ranks strictly worse than
/// the `k`-th winner, no unseen row of `t` can enter the top-`k`. When `t` is
/// exhausted it has no unseen rows at all. With every tablet in one of those
/// two states, the received top-`k` is the global top-`k`. When fewer than
/// `k` candidates were received, refill is empty iff every tablet is
/// exhausted, in which case all rows were seen.
pub fn merge_top_k(shards: &[TabletTopK], k: usize) -> TopKMerge {
    if k == 0 {
        return TopKMerge {
            winners: Vec::new(),
            refill: Vec::new(),
        };
    }
    let mut received: Vec<TopKCandidate> = shards
        .iter()
        .flat_map(|shard| shard.rows.iter().copied())
        .collect();
    received.sort_by(topk_cmp);
    received.truncate(k);
    let mut refill = Vec::new();
    if received.len() < k {
        for shard in shards {
            if shard.unseen_bound.is_some() {
                refill.push(shard.tablet);
            }
        }
    } else {
        let threshold = received[k - 1];
        for shard in shards {
            let Some(bound) = shard.unseen_bound else {
                continue;
            };
            let optimistic = TopKCandidate {
                score: bound,
                tablet: shard.tablet,
                row_id: RowId::MIN,
            };
            if topk_cmp(&optimistic, &threshold) != Ordering::Greater {
                refill.push(shard.tablet);
            }
        }
    }
    refill.sort();
    refill.dedup();
    TopKMerge {
        winners: received,
        refill,
    }
}

/// Drives [`merge_top_k`] with adaptive refill until the result is provably
/// exact (spec section 12.10: "for exact global top-k, use adaptive refill
/// when local bounds show unseen rows could still win").
///
/// `initial` holds each tablet's first bounded contribution. `refill_batch`
/// must return the NEXT batch of local winners for one tablet (rows not
/// returned before, best first) together with a tightened unseen bound
/// (`None` when the tablet is exhausted). Iteration order over tablets is
/// sorted by id, so the driver is fully deterministic. Fails when a refill
/// makes no progress (no new rows and an unchanged bound), which indicates a
/// broken producer contract rather than a planning problem.
pub fn exact_top_k(
    k: usize,
    initial: Vec<TabletTopK>,
    mut refill_batch: impl FnMut(TabletId) -> TabletTopK,
) -> DistributedResult<Vec<TopKCandidate>> {
    let mut shards: BTreeMap<TabletId, TabletTopK> = initial
        .into_iter()
        .map(|shard| (shard.tablet, shard))
        .collect();
    loop {
        let ordered: Vec<TabletTopK> = shards.values().cloned().collect();
        let merge = merge_top_k(&ordered, k);
        if merge.refill.is_empty() {
            return Ok(merge.winners);
        }
        for tablet in merge.refill {
            let batch = refill_batch(tablet);
            let entry = shards.get_mut(&tablet).ok_or_else(|| {
                DistributedError::InvalidPlan(format!(
                    "top-k refill requested for unknown tablet {tablet}"
                ))
            })?;
            if batch.rows.is_empty() && batch.unseen_bound == entry.unseen_bound {
                return Err(DistributedError::InvalidPlan(format!(
                    "top-k refill for tablet {tablet} made no progress"
                )));
            }
            entry.rows.extend(batch.rows);
            entry.unseen_bound = batch.unseen_bound;
        }
    }
}

// ---------------------------------------------------------------------------
// Execution skeleton (spec section 12.10)
// ---------------------------------------------------------------------------

/// Cooperative per-fragment execution control: cancellation/deadline shared
/// with the query's [`ExecutionControl`] hierarchy plus the fragment's spill
/// allowance.
#[derive(Debug, Clone)]
pub struct FragmentControl {
    /// Cooperative cancellation handle (child of the query control, so a
    /// registry cancel fans out to every fragment).
    pub control: ExecutionControl,
    /// Maximum spill allowance stamped by the planner (spec section 12.10).
    pub max_spill_bytes: u64,
    /// Opaque server-issued user/session authorization envelope. Worker
    /// executors validate it before touching tablet data.
    pub authorization_context: Arc<[u8]>,
}

impl FragmentControl {
    /// Open a core spill session for this fragment against `manager`, capped
    /// at [`Self::max_spill_bytes`]. Query operators that sort/join/aggregate
    /// under pressure call this instead of inventing a second spill path.
    pub fn begin_spill(
        &self,
        manager: &mongreldb_core::SpillManager,
        query_id: mongreldb_types::ids::QueryId,
    ) -> Result<mongreldb_core::SpillSession, mongreldb_core::SpillError> {
        manager.begin_query(query_id, self.max_spill_bytes)
    }
}

/// Tie information for scored (top-k) streams, carried on a producer's
/// terminal frame (spec section 12.10: "bounded local top-k plus tie
/// information").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScoreBound {
    /// Not a scored stream, or the producer cannot report bounds; the
    /// coordinator treats such producers as exhausted.
    Unknown,
    /// Unsent rows may have score keys up to this value.
    AtMost(u64),
    /// The producer emitted every local row.
    Exhausted,
}

/// One Arrow-ish record-batch frame on a fragment stream.
#[derive(Debug, Clone)]
pub struct BatchFrame {
    /// The record batch payload.
    pub batch: RecordBatch,
    /// Tie information; meaningful only on a scored stream's terminal frame.
    pub score_bound: ScoreBound,
}

impl BatchFrame {
    /// A plain data frame (no tie information).
    pub fn data(batch: RecordBatch) -> Self {
        Self {
            batch,
            score_bound: ScoreBound::Unknown,
        }
    }
}

/// A fragment's output stream.
pub type FragmentStream = BoxStream<'static, DistributedResult<BatchFrame>>;

/// The next batch of a tablet's local top-k (adaptive refill, spec section
/// 12.10).
#[derive(Debug)]
pub struct TopKRefill {
    /// The next local candidates (rows not returned before, best first),
    /// aligned with `payload`'s rows.
    pub rows: Vec<TopKCandidate>,
    /// The candidates' full row payload (same schema as the stream output).
    pub payload: RecordBatch,
    /// Tightened unseen-score bound (`None` = tablet exhausted).
    pub unseen_bound: Option<u64>,
}

/// Executes one fragment on its worker. Server/engine bindings install an
/// implementation behind [`RemoteFragmentEndpoint`];
/// [`InMemoryFragmentExecutor`] remains the deterministic reference executor.
#[async_trait::async_trait]
pub trait FragmentExecutor: Send + Sync {
    /// Runs the fragment. `inputs` carries one resolved stream per
    /// [`FragmentOperator::RemoteExchangeSource`] operator, in operator order.
    async fn execute(
        &self,
        fragment: &PlanFragment,
        inputs: Vec<FragmentStream>,
        control: FragmentControl,
    ) -> DistributedResult<FragmentStream>;

    /// Returns the next `limit` local top-k candidates after `offset` (the
    /// number already returned), with a tightened unseen bound. Executors
    /// without scored streams leave the default, which rejects.
    fn refill_top_k(
        &self,
        fragment: &PlanFragment,
        offset: usize,
        limit: usize,
        control: FragmentControl,
    ) -> DistributedResult<TopKRefill> {
        let _ = (fragment, offset, limit, control);
        Err(DistributedError::Unsupported(
            "top-k refill is not implemented by this executor".to_owned(),
        ))
    }
}

/// Moves fragments, refill, and cancellation between the coordinator and
/// workers (spec section 12.10). Implementations include the deterministic
/// [`InMemoryTransport`] and the bounded Arrow IPC
/// [`RemoteFragmentTransport`].
#[async_trait::async_trait]
pub trait FragmentTransport: Send + Sync {
    /// Starts a fragment on its assigned worker and returns its output
    /// stream.
    async fn execute_fragment(
        &self,
        query_id: QueryId,
        fragment: &PlanFragment,
        inputs: Vec<FragmentStream>,
        control: FragmentControl,
    ) -> DistributedResult<FragmentStream>;

    /// Best-effort cancellation of a running (or abandoned) fragment.
    fn cancel_fragment(&self, query_id: QueryId, fragment_id: FragmentId) -> DistributedResult<()>;

    /// Fetches the next top-k batch of a fragment's tablet (adaptive
    /// refill). Transports without a refill binding leave the default.
    async fn refill_top_k(
        &self,
        query_id: QueryId,
        fragment: &PlanFragment,
        offset: usize,
        limit: usize,
        control: FragmentControl,
    ) -> DistributedResult<TopKRefill> {
        let _ = (query_id, fragment, offset, limit, control);
        Err(DistributedError::Unsupported(
            "top-k refill over this transport is not bound in this wave".to_owned(),
        ))
    }
}

/// In-memory [`FragmentTransport`]: routes fragments to per-tablet executors,
/// records starts/cancellations/refills for test introspection, and keeps
/// each fragment's [`ExecutionControl`] so cancellations are observable.
pub struct InMemoryTransport {
    default_executor: Arc<dyn FragmentExecutor>,
    executors: parking_lot::RwLock<HashMap<TabletId, Arc<dyn FragmentExecutor>>>,
    started: Mutex<Vec<FragmentId>>,
    cancelled: Mutex<Vec<FragmentId>>,
    controls: Mutex<HashMap<FragmentId, ExecutionControl>>,
    refills: Mutex<Vec<(FragmentId, usize, usize)>>,
}

impl InMemoryTransport {
    /// A transport whose every fragment runs on `default_executor`.
    pub fn new(default_executor: Arc<dyn FragmentExecutor>) -> Self {
        Self {
            default_executor,
            executors: parking_lot::RwLock::new(HashMap::new()),
            started: Mutex::new(Vec::new()),
            cancelled: Mutex::new(Vec::new()),
            controls: Mutex::new(HashMap::new()),
            refills: Mutex::new(Vec::new()),
        }
    }

    /// Pins one tablet to its own executor.
    pub fn with_executor(self, tablet: TabletId, executor: Arc<dyn FragmentExecutor>) -> Self {
        self.executors.write().insert(tablet, executor);
        self
    }

    fn executor_for(&self, assignment: &FragmentAssignment) -> Arc<dyn FragmentExecutor> {
        match assignment {
            FragmentAssignment::Tablet(tablet) => self
                .executors
                .read()
                .get(tablet)
                .cloned()
                .unwrap_or_else(|| Arc::clone(&self.default_executor)),
            FragmentAssignment::Coordinator => Arc::clone(&self.default_executor),
        }
    }

    /// Fragments started so far, in start order.
    pub fn started_fragments(&self) -> Vec<FragmentId> {
        self.started.lock().clone()
    }

    /// Fragments cancelled so far, in cancel order.
    pub fn cancelled_fragments(&self) -> Vec<FragmentId> {
        self.cancelled.lock().clone()
    }

    /// Top-k refills issued so far: `(fragment_id, offset, limit)`.
    pub fn refill_log(&self) -> Vec<(FragmentId, usize, usize)> {
        self.refills.lock().clone()
    }

    /// The control handed to a started fragment, for cancellation assertions.
    pub fn control_for(&self, fragment_id: FragmentId) -> Option<ExecutionControl> {
        self.controls.lock().get(&fragment_id).cloned()
    }
}

#[async_trait::async_trait]
impl FragmentTransport for InMemoryTransport {
    async fn execute_fragment(
        &self,
        _query_id: QueryId,
        fragment: &PlanFragment,
        inputs: Vec<FragmentStream>,
        control: FragmentControl,
    ) -> DistributedResult<FragmentStream> {
        self.started.lock().push(fragment.fragment_id);
        self.controls
            .lock()
            .insert(fragment.fragment_id, control.control.clone());
        self.executor_for(&fragment.assignment)
            .execute(fragment, inputs, control)
            .await
    }

    fn cancel_fragment(
        &self,
        _query_id: QueryId,
        fragment_id: FragmentId,
    ) -> DistributedResult<()> {
        self.cancelled.lock().push(fragment_id);
        if let Some(control) = self.controls.lock().get(&fragment_id) {
            control.cancel(CancellationReason::ClientRequest);
        }
        Ok(())
    }

    async fn refill_top_k(
        &self,
        _query_id: QueryId,
        fragment: &PlanFragment,
        offset: usize,
        limit: usize,
        control: FragmentControl,
    ) -> DistributedResult<TopKRefill> {
        self.refills
            .lock()
            .push((fragment.fragment_id, offset, limit));
        self.executor_for(&fragment.assignment)
            .refill_top_k(fragment, offset, limit, control)
    }
}

// ---------------------------------------------------------------------------
// Remote Arrow IPC fragment transport
// ---------------------------------------------------------------------------

/// Version of the private cluster fragment protocol.
///
/// This is an internal wire generation, not a MongrelDB release or database
/// format version. Peers fail closed when it differs.
pub const REMOTE_FRAGMENT_PROTOCOL_VERSION: u16 = 1;
/// Stable service discriminator inside the cluster's authenticated internal
/// RPC multiplexer.
pub const REMOTE_FRAGMENT_SERVICE_ID: u32 = 1;

/// Default maximum encoded request or response body for one fragment RPC.
pub const DEFAULT_REMOTE_FRAGMENT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Default maximum number of fragment streams held by one worker.
pub const DEFAULT_REMOTE_FRAGMENT_EXECUTIONS: usize = 1_024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteFragmentEnvelope {
    version: u16,
    request: RemoteFragmentRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoteFragmentRequest {
    Start {
        query_id: QueryId,
        fragment: PlanFragment,
        inputs: Vec<Vec<RemoteBatchFrame>>,
        max_spill_bytes: u64,
        authorization_context: Vec<u8>,
        deadline_ms: Option<u64>,
    },
    Pull {
        query_id: QueryId,
        fragment_id: FragmentId,
    },
    Cancel {
        query_id: QueryId,
        fragment_id: FragmentId,
    },
    RefillTopK {
        query_id: QueryId,
        fragment: PlanFragment,
        offset: usize,
        limit: usize,
        authorization_context: Vec<u8>,
        deadline_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteFragmentResponseEnvelope {
    version: u16,
    response: RemoteFragmentResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoteFragmentResponse {
    Started,
    Frame(Option<RemoteBatchFrame>),
    Cancelled,
    TopKRefill {
        rows: Vec<TopKCandidate>,
        payload: Vec<u8>,
        unseen_bound: Option<u64>,
    },
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteBatchFrame {
    ipc: Vec<u8>,
    score_bound: ScoreBound,
}

type RemoteExecutionKey = (QueryId, FragmentId);

struct RemoteExecution {
    stream: tokio::sync::Mutex<Option<FragmentStream>>,
    control: ExecutionControl,
}

/// Lifecycle counters for remote fragment workers (P0.4-T6).
#[derive(Debug, Default)]
pub struct FragmentLifecycleMetrics {
    /// Fragment start requests accepted.
    pub starts: std::sync::atomic::AtomicU64,
    /// Pull frames returned (including end-of-stream).
    pub pulls: std::sync::atomic::AtomicU64,
    /// Explicit cancel requests that reclaimed a cursor.
    pub cancels: std::sync::atomic::AtomicU64,
    /// Streams that completed (end-of-stream pull).
    pub completes: std::sync::atomic::AtomicU64,
    /// Request body bytes handled.
    pub bytes_in: std::sync::atomic::AtomicU64,
    /// Response body bytes emitted.
    pub bytes_out: std::sync::atomic::AtomicU64,
}

impl FragmentLifecycleMetrics {
    /// Snapshot of counters for tests / admin surfaces.
    pub fn snapshot(&self) -> FragmentLifecycleSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        FragmentLifecycleSnapshot {
            starts: self.starts.load(Relaxed),
            pulls: self.pulls.load(Relaxed),
            cancels: self.cancels.load(Relaxed),
            completes: self.completes.load(Relaxed),
            bytes_in: self.bytes_in.load(Relaxed),
            bytes_out: self.bytes_out.load(Relaxed),
            active_executions: 0,
        }
    }
}

/// Point-in-time fragment lifecycle view (P0.4-T6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FragmentLifecycleSnapshot {
    pub starts: u64,
    pub pulls: u64,
    pub cancels: u64,
    pub completes: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub active_executions: usize,
}

/// Worker-side endpoint for the internal fragment protocol.
///
/// The cluster transport supplies authenticated node-to-node delivery. This
/// endpoint owns bounded active cursors and returns one Arrow IPC batch per
/// pull. Pull framing provides backpressure without buffering a whole result
/// on either peer.
pub struct RemoteFragmentEndpoint {
    executor: Arc<dyn FragmentExecutor>,
    executions: parking_lot::Mutex<HashMap<RemoteExecutionKey, Arc<RemoteExecution>>>,
    max_executions: usize,
    max_message_bytes: usize,
    metrics: FragmentLifecycleMetrics,
}

impl RemoteFragmentEndpoint {
    /// Creates a bounded worker endpoint.
    pub fn new(executor: Arc<dyn FragmentExecutor>) -> Self {
        Self::with_limits(
            executor,
            DEFAULT_REMOTE_FRAGMENT_EXECUTIONS,
            DEFAULT_REMOTE_FRAGMENT_MESSAGE_BYTES,
        )
    }

    /// Creates a worker endpoint with explicit cursor and frame bounds.
    pub fn with_limits(
        executor: Arc<dyn FragmentExecutor>,
        max_executions: usize,
        max_message_bytes: usize,
    ) -> Self {
        Self {
            executor,
            executions: parking_lot::Mutex::new(HashMap::new()),
            max_executions: max_executions.max(1),
            max_message_bytes: max_message_bytes.max(1),
            metrics: FragmentLifecycleMetrics::default(),
        }
    }

    /// Number of live remote fragment cursors.
    pub fn active_executions(&self) -> usize {
        self.executions.lock().len()
    }

    /// Fragment lifecycle metrics including active cursor count (P0.4-T6).
    pub fn lifecycle_metrics(&self) -> FragmentLifecycleSnapshot {
        let mut snap = self.metrics.snapshot();
        snap.active_executions = self.active_executions();
        snap
    }

    /// Handles one authenticated internal RPC body.
    pub async fn handle(&self, bytes: &[u8]) -> DistributedResult<Vec<u8>> {
        use std::sync::atomic::Ordering::Relaxed;
        self.metrics.bytes_in.fetch_add(bytes.len() as u64, Relaxed);
        if bytes.len() > self.max_message_bytes {
            return Err(DistributedError::RemoteProtocol(format!(
                "fragment request is {} bytes; limit is {}",
                bytes.len(),
                self.max_message_bytes
            )));
        }
        let envelope: RemoteFragmentEnvelope = decode_remote_wire(bytes, self.max_message_bytes)?;
        if envelope.version != REMOTE_FRAGMENT_PROTOCOL_VERSION {
            return Err(DistributedError::RemoteProtocol(format!(
                "unsupported fragment protocol version {}; supported version is {}",
                envelope.version, REMOTE_FRAGMENT_PROTOCOL_VERSION
            )));
        }
        let response = match self.handle_request(envelope.request).await {
            Ok(response) => response,
            Err(error) => RemoteFragmentResponse::Error(error.to_string()),
        };
        let encoded = encode_remote_wire(
            &RemoteFragmentResponseEnvelope {
                version: REMOTE_FRAGMENT_PROTOCOL_VERSION,
                response,
            },
            self.max_message_bytes,
        )?;
        self.metrics
            .bytes_out
            .fetch_add(encoded.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(encoded)
    }

    async fn handle_request(
        &self,
        request: RemoteFragmentRequest,
    ) -> DistributedResult<RemoteFragmentResponse> {
        match request {
            RemoteFragmentRequest::Start {
                query_id,
                fragment,
                inputs,
                max_spill_bytes,
                authorization_context,
                deadline_ms,
            } => {
                if authorization_context.len() > MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES {
                    return Err(DistributedError::RemoteProtocol(format!(
                        "fragment authorization context is {} bytes; limit is {}",
                        authorization_context.len(),
                        MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES
                    )));
                }
                let key = (query_id, fragment.fragment_id);
                {
                    let executions = self.executions.lock();
                    if executions.contains_key(&key) {
                        return Err(DistributedError::RemoteProtocol(format!(
                            "fragment {} for query {query_id} is already running",
                            fragment.fragment_id
                        )));
                    }
                    if executions.len() >= self.max_executions {
                        return Err(DistributedError::Reservation {
                            fragment_id: fragment.fragment_id,
                            reason: format!(
                                "worker holds {} remote fragments; limit is {}",
                                executions.len(),
                                self.max_executions
                            ),
                        });
                    }
                }
                let inputs = inputs
                    .into_iter()
                    .map(|frames| {
                        frames
                            .into_iter()
                            .map(|frame| decode_remote_frame(frame, self.max_message_bytes))
                            .collect::<DistributedResult<Vec<_>>>()
                            .map(|frames| {
                                Box::pin(stream::iter(frames.into_iter().map(Ok))) as FragmentStream
                            })
                    })
                    .collect::<DistributedResult<Vec<_>>>()?;
                let control = deadline_ms.map_or_else(
                    || ExecutionControl::new(None),
                    |milliseconds| {
                        ExecutionControl::with_timeout(Duration::from_millis(milliseconds))
                    },
                );
                let execution = Arc::new(RemoteExecution {
                    stream: tokio::sync::Mutex::new(None),
                    control: control.clone(),
                });
                {
                    let mut executions = self.executions.lock();
                    if executions.contains_key(&key) {
                        return Err(DistributedError::RemoteProtocol(format!(
                            "fragment {} for query {query_id} raced another start",
                            fragment.fragment_id
                        )));
                    }
                    if executions.len() >= self.max_executions {
                        return Err(DistributedError::Reservation {
                            fragment_id: fragment.fragment_id,
                            reason: "remote fragment limit reached during start".to_owned(),
                        });
                    }
                    executions.insert(key, Arc::clone(&execution));
                }
                let stream = match self
                    .executor
                    .execute(
                        &fragment,
                        inputs,
                        FragmentControl {
                            control,
                            max_spill_bytes,
                            authorization_context: authorization_context.into(),
                        },
                    )
                    .await
                {
                    Ok(stream) => stream,
                    Err(error) => {
                        self.executions.lock().remove(&key);
                        return Err(error);
                    }
                };
                if self.executions.lock().get(&key).is_none() {
                    return Err(DistributedError::Cancelled(
                        CancellationReason::ClientRequest,
                    ));
                }
                if let Err(error) = checkpoint(&execution.control) {
                    self.executions.lock().remove(&key);
                    return Err(error);
                }
                *execution.stream.lock().await = Some(stream);
                self.metrics
                    .starts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(RemoteFragmentResponse::Started)
            }
            RemoteFragmentRequest::Pull {
                query_id,
                fragment_id,
            } => {
                let key = (query_id, fragment_id);
                let execution = self.executions.lock().get(&key).cloned().ok_or_else(|| {
                    DistributedError::RemoteProtocol(format!(
                        "fragment {fragment_id} for query {query_id} is not running"
                    ))
                })?;
                let next = {
                    checkpoint(&execution.control)?;
                    let mut stream = execution.stream.lock().await;
                    let stream = stream.as_mut().ok_or_else(|| {
                        DistributedError::RemoteProtocol(format!(
                            "fragment {fragment_id} for query {query_id} is not ready"
                        ))
                    })?;
                    stream.next().await.transpose()?
                };
                self.metrics
                    .pulls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match next {
                    Some(frame) => Ok(RemoteFragmentResponse::Frame(Some(encode_remote_frame(
                        &frame,
                        self.max_message_bytes,
                    )?))),
                    None => {
                        self.executions.lock().remove(&key);
                        self.metrics
                            .completes
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(RemoteFragmentResponse::Frame(None))
                    }
                }
            }
            RemoteFragmentRequest::Cancel {
                query_id,
                fragment_id,
            } => {
                if let Some(execution) = self.executions.lock().remove(&(query_id, fragment_id)) {
                    execution.control.cancel(CancellationReason::ClientRequest);
                    self.metrics
                        .cancels
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(RemoteFragmentResponse::Cancelled)
            }
            RemoteFragmentRequest::RefillTopK {
                query_id: _,
                fragment,
                offset,
                limit,
                authorization_context,
                deadline_ms,
            } => {
                if authorization_context.len() > MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES {
                    return Err(DistributedError::RemoteProtocol(format!(
                        "fragment authorization context is {} bytes; limit is {}",
                        authorization_context.len(),
                        MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES
                    )));
                }
                let control = deadline_ms.map_or_else(
                    || ExecutionControl::new(None),
                    |milliseconds| {
                        ExecutionControl::with_timeout(Duration::from_millis(milliseconds))
                    },
                );
                let refill = self.executor.refill_top_k(
                    &fragment,
                    offset,
                    limit,
                    FragmentControl {
                        control,
                        max_spill_bytes: fragment.max_spill_bytes,
                        authorization_context: authorization_context.into(),
                    },
                )?;
                let payload = encode_record_batch(&refill.payload, self.max_message_bytes)?;
                Ok(RemoteFragmentResponse::TopKRefill {
                    rows: refill.rows,
                    payload,
                    unseen_bound: refill.unseen_bound,
                })
            }
        }
    }
}

/// One authenticated request/response carrier for remote fragment bodies.
///
/// Production implements this over the cluster's node-identity-bound mTLS
/// transport. Tests may use [`LoopbackFragmentRpcClient`] to isolate the
/// query-side protocol.
#[async_trait::async_trait]
pub trait FragmentRpcClient: Send + Sync {
    /// Performs one bounded internal RPC.
    async fn call(&self, request: Vec<u8>) -> DistributedResult<Vec<u8>>;
}

/// In-process carrier for protocol and endpoint tests.
pub struct LoopbackFragmentRpcClient {
    endpoint: Arc<RemoteFragmentEndpoint>,
}

impl LoopbackFragmentRpcClient {
    /// Wraps one worker endpoint.
    pub fn new(endpoint: Arc<RemoteFragmentEndpoint>) -> Self {
        Self { endpoint }
    }
}

#[async_trait::async_trait]
impl FragmentRpcClient for LoopbackFragmentRpcClient {
    async fn call(&self, request: Vec<u8>) -> DistributedResult<Vec<u8>> {
        self.endpoint.handle(&request).await
    }
}

/// Coordinator-side Arrow IPC transport for tablet fragments.
///
/// Each tablet route names an authenticated RPC carrier. Output is pulled one
/// batch at a time, so consumer polling is the backpressure mechanism.
pub struct RemoteFragmentTransport {
    default_client: Option<Arc<dyn FragmentRpcClient>>,
    clients: parking_lot::RwLock<HashMap<TabletId, Arc<dyn FragmentRpcClient>>>,
    active: Arc<parking_lot::Mutex<HashMap<RemoteExecutionKey, Arc<dyn FragmentRpcClient>>>>,
    max_message_bytes: usize,
}

impl RemoteFragmentTransport {
    /// Creates a transport whose tablets use `default_client`.
    pub fn new(default_client: Arc<dyn FragmentRpcClient>) -> Self {
        Self::with_message_limit(default_client, DEFAULT_REMOTE_FRAGMENT_MESSAGE_BYTES)
    }

    /// Creates a transport with an explicit per-call body bound.
    pub fn with_message_limit(
        default_client: Arc<dyn FragmentRpcClient>,
        max_message_bytes: usize,
    ) -> Self {
        Self {
            default_client: Some(default_client),
            clients: parking_lot::RwLock::new(HashMap::new()),
            active: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            max_message_bytes: max_message_bytes.max(1),
        }
    }

    /// Creates a fail-closed transport with no fallback route.
    pub fn routed() -> Self {
        Self {
            default_client: None,
            clients: parking_lot::RwLock::new(HashMap::new()),
            active: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            max_message_bytes: DEFAULT_REMOTE_FRAGMENT_MESSAGE_BYTES,
        }
    }

    /// Routes one tablet through a specific peer carrier.
    pub fn with_client(self, tablet: TabletId, client: Arc<dyn FragmentRpcClient>) -> Self {
        self.clients.write().insert(tablet, client);
        self
    }

    fn client_for(&self, fragment: &PlanFragment) -> DistributedResult<Arc<dyn FragmentRpcClient>> {
        let tablet = tablet_of(fragment)?;
        self.clients
            .read()
            .get(&tablet)
            .cloned()
            .or_else(|| self.default_client.as_ref().map(Arc::clone))
            .ok_or_else(|| {
                DistributedError::RemoteTransport(format!(
                    "no authenticated fragment route for tablet {tablet}"
                ))
            })
    }

    async fn call(
        &self,
        client: &Arc<dyn FragmentRpcClient>,
        request: RemoteFragmentRequest,
    ) -> DistributedResult<RemoteFragmentResponse> {
        remote_call(client, request, self.max_message_bytes).await
    }
}

struct RemoteStreamState {
    client: Arc<dyn FragmentRpcClient>,
    key: RemoteExecutionKey,
    active: Arc<parking_lot::Mutex<HashMap<RemoteExecutionKey, Arc<dyn FragmentRpcClient>>>>,
    max_message_bytes: usize,
    complete: bool,
    yielded_error: bool,
}

impl RemoteStreamState {
    fn mark_complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for RemoteStreamState {
    fn drop(&mut self) {
        self.active.lock().remove(&self.key);
        if self.complete {
            return;
        }
        let request = RemoteFragmentRequest::Cancel {
            query_id: self.key.0,
            fragment_id: self.key.1,
        };
        let client = Arc::clone(&self.client);
        let max_message_bytes = self.max_message_bytes;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = remote_call(&client, request, max_message_bytes).await;
            });
        }
    }
}

#[async_trait::async_trait]
impl FragmentTransport for RemoteFragmentTransport {
    async fn execute_fragment(
        &self,
        query_id: QueryId,
        fragment: &PlanFragment,
        inputs: Vec<FragmentStream>,
        control: FragmentControl,
    ) -> DistributedResult<FragmentStream> {
        let client = self.client_for(fragment)?;
        let mut wire_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            let frames = drain_stream(input, &control.control).await?;
            wire_inputs.push(
                frames
                    .iter()
                    .map(|frame| encode_remote_frame(frame, self.max_message_bytes))
                    .collect::<DistributedResult<Vec<_>>>()?,
            );
        }
        let key = (query_id, fragment.fragment_id);
        self.active.lock().insert(key, Arc::clone(&client));
        let start = self.call(
            &client,
            RemoteFragmentRequest::Start {
                query_id,
                fragment: fragment.clone(),
                inputs: wire_inputs,
                max_spill_bytes: control.max_spill_bytes,
                authorization_context: control.authorization_context.to_vec(),
                deadline_ms: control
                    .control
                    .remaining_duration()
                    .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
            },
        );
        let response = tokio::select! {
            response = start => response,
            _ = control.control.cancelled() => {
                let _ = remote_call(
                    &client,
                    RemoteFragmentRequest::Cancel {
                        query_id,
                        fragment_id: fragment.fragment_id,
                    },
                    self.max_message_bytes,
                ).await;
                Err(DistributedError::Cancelled(control.control.reason()))
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.active.lock().remove(&key);
                return Err(error);
            }
        };
        match response {
            RemoteFragmentResponse::Started => {}
            other => {
                self.active.lock().remove(&key);
                return Err(unexpected_remote_response("Started", &other));
            }
        }
        let state = RemoteStreamState {
            client,
            key,
            active: Arc::clone(&self.active),
            max_message_bytes: self.max_message_bytes,
            complete: false,
            yielded_error: false,
        };
        let output = stream::unfold(state, |mut state| async move {
            if state.complete || state.yielded_error {
                return None;
            }
            let response = remote_call(
                &state.client,
                RemoteFragmentRequest::Pull {
                    query_id: state.key.0,
                    fragment_id: state.key.1,
                },
                state.max_message_bytes,
            )
            .await;
            match response {
                Ok(RemoteFragmentResponse::Frame(Some(frame))) => {
                    let item = decode_remote_frame(frame, state.max_message_bytes);
                    if item.is_err() {
                        state.yielded_error = true;
                    }
                    Some((item, state))
                }
                Ok(RemoteFragmentResponse::Frame(None)) => {
                    state.mark_complete();
                    None
                }
                Ok(other) => {
                    state.yielded_error = true;
                    Some((Err(unexpected_remote_response("Frame", &other)), state))
                }
                Err(error) => {
                    state.yielded_error = true;
                    Some((Err(error), state))
                }
            }
        });
        Ok(Box::pin(output))
    }

    fn cancel_fragment(&self, query_id: QueryId, fragment_id: FragmentId) -> DistributedResult<()> {
        let key = (query_id, fragment_id);
        let Some(client) = self.active.lock().remove(&key) else {
            return Ok(());
        };
        let max_message_bytes = self.max_message_bytes;
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            DistributedError::RemoteTransport(format!(
                "cannot schedule fragment cancellation outside Tokio: {error}"
            ))
        })?;
        runtime.spawn(async move {
            let _ = remote_call(
                &client,
                RemoteFragmentRequest::Cancel {
                    query_id,
                    fragment_id,
                },
                max_message_bytes,
            )
            .await;
        });
        Ok(())
    }

    async fn refill_top_k(
        &self,
        query_id: QueryId,
        fragment: &PlanFragment,
        offset: usize,
        limit: usize,
        control: FragmentControl,
    ) -> DistributedResult<TopKRefill> {
        let client = self.client_for(fragment)?;
        match self
            .call(
                &client,
                RemoteFragmentRequest::RefillTopK {
                    query_id,
                    fragment: fragment.clone(),
                    offset,
                    limit,
                    authorization_context: control.authorization_context.to_vec(),
                    deadline_ms: control
                        .control
                        .remaining_duration()
                        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
                },
            )
            .await?
        {
            RemoteFragmentResponse::TopKRefill {
                rows,
                payload,
                unseen_bound,
            } => Ok(TopKRefill {
                rows,
                payload: decode_record_batch(&payload, self.max_message_bytes)?,
                unseen_bound,
            }),
            other => Err(unexpected_remote_response("TopKRefill", &other)),
        }
    }
}

async fn remote_call(
    client: &Arc<dyn FragmentRpcClient>,
    request: RemoteFragmentRequest,
    max_message_bytes: usize,
) -> DistributedResult<RemoteFragmentResponse> {
    let request = encode_remote_wire(
        &RemoteFragmentEnvelope {
            version: REMOTE_FRAGMENT_PROTOCOL_VERSION,
            request,
        },
        max_message_bytes,
    )?;
    let response = client.call(request).await?;
    let envelope: RemoteFragmentResponseEnvelope =
        decode_remote_wire(&response, max_message_bytes)?;
    if envelope.version != REMOTE_FRAGMENT_PROTOCOL_VERSION {
        return Err(DistributedError::RemoteProtocol(format!(
            "peer answered with fragment protocol version {}; supported version is {}",
            envelope.version, REMOTE_FRAGMENT_PROTOCOL_VERSION
        )));
    }
    match envelope.response {
        RemoteFragmentResponse::Error(message) => Err(DistributedError::RemoteTransport(message)),
        response => Ok(response),
    }
}

fn remote_wire_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

fn encode_remote_wire<T: Serialize>(
    value: &T,
    max_message_bytes: usize,
) -> DistributedResult<Vec<u8>> {
    let bytes = remote_wire_options()
        .serialize(value)
        .map_err(|error| DistributedError::RemoteProtocol(error.to_string()))?;
    if bytes.len() > max_message_bytes {
        return Err(DistributedError::RemoteProtocol(format!(
            "encoded fragment message is {} bytes; limit is {max_message_bytes}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn decode_remote_wire<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    max_message_bytes: usize,
) -> DistributedResult<T> {
    if bytes.len() > max_message_bytes {
        return Err(DistributedError::RemoteProtocol(format!(
            "fragment message is {} bytes; limit is {max_message_bytes}",
            bytes.len()
        )));
    }
    remote_wire_options()
        .with_limit(max_message_bytes as u64)
        .deserialize(bytes)
        .map_err(|error| DistributedError::RemoteProtocol(error.to_string()))
}

fn encode_record_batch(
    batch: &RecordBatch,
    max_message_bytes: usize,
) -> DistributedResult<Vec<u8>> {
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    if ipc.len() > max_message_bytes {
        return Err(DistributedError::RemoteProtocol(format!(
            "Arrow IPC batch is {} bytes; limit is {max_message_bytes}",
            ipc.len()
        )));
    }
    Ok(ipc)
}

fn decode_record_batch(ipc: &[u8], max_message_bytes: usize) -> DistributedResult<RecordBatch> {
    if ipc.len() > max_message_bytes {
        return Err(DistributedError::RemoteProtocol(format!(
            "Arrow IPC batch is {} bytes; limit is {max_message_bytes}",
            ipc.len()
        )));
    }
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None)?;
    let batch = reader.next().transpose()?.ok_or_else(|| {
        DistributedError::RemoteProtocol("Arrow IPC frame contains no record batch".to_owned())
    })?;
    if reader.next().transpose()?.is_some() {
        return Err(DistributedError::RemoteProtocol(
            "Arrow IPC frame contains more than one record batch".to_owned(),
        ));
    }
    Ok(batch)
}

fn encode_remote_frame(
    frame: &BatchFrame,
    max_message_bytes: usize,
) -> DistributedResult<RemoteBatchFrame> {
    Ok(RemoteBatchFrame {
        ipc: encode_record_batch(&frame.batch, max_message_bytes)?,
        score_bound: frame.score_bound,
    })
}

fn decode_remote_frame(
    frame: RemoteBatchFrame,
    max_message_bytes: usize,
) -> DistributedResult<BatchFrame> {
    Ok(BatchFrame {
        batch: decode_record_batch(&frame.ipc, max_message_bytes)?,
        score_bound: frame.score_bound,
    })
}

fn unexpected_remote_response(expected: &str, actual: &RemoteFragmentResponse) -> DistributedError {
    DistributedError::RemoteProtocol(format!(
        "expected remote {expected} response, got {actual:?}"
    ))
}

/// Batch source consumed by the reference fragment operator engine.
///
/// Implementations validate `control.authorization_context` before reading.
pub trait FragmentTableSource: Send + Sync {
    /// Reads one table slice from a hosted tablet.
    fn scan(
        &self,
        table: &str,
        tablet: TabletId,
        include_row_id: bool,
        control: Option<&FragmentControl>,
    ) -> DistributedResult<Vec<RecordBatch>>;

    /// Returns the empty-result schema for one table, when known.
    fn schema(&self, table: &str, tablet: TabletId) -> DistributedResult<Option<SchemaRef>>;
}

/// Resolves the local storage owner for one hosted tablet.
pub trait FragmentDatabaseProvider: Send + Sync {
    /// Returns `None` when this node does not host `tablet`.
    fn database(&self, tablet: TabletId) -> Option<Arc<mongreldb_core::Database>>;
}

/// Validates the forwarded server-issued authorization envelope.
pub trait FragmentAuthorizationResolver: Send + Sync {
    /// Resolves the exact principal for the local database. `None` is valid
    /// only for a credentialless database.
    fn resolve(
        &self,
        database: &mongreldb_core::Database,
        context: &[u8],
    ) -> DistributedResult<Option<mongreldb_core::Principal>>;
}

/// Core MVCC-backed tablet source used by remote SQL workers.
pub struct CoreFragmentTableSource {
    databases: Arc<dyn FragmentDatabaseProvider>,
    authorization: Arc<dyn FragmentAuthorizationResolver>,
}

impl CoreFragmentTableSource {
    /// Creates an authorized core-backed source.
    pub fn new(
        databases: Arc<dyn FragmentDatabaseProvider>,
        authorization: Arc<dyn FragmentAuthorizationResolver>,
    ) -> Self {
        Self {
            databases,
            authorization,
        }
    }
}

impl FragmentTableSource for CoreFragmentTableSource {
    fn scan(
        &self,
        table: &str,
        tablet: TabletId,
        include_row_id: bool,
        control: Option<&FragmentControl>,
    ) -> DistributedResult<Vec<RecordBatch>> {
        let control = control.ok_or_else(|| {
            DistributedError::RemoteProtocol(
                "core tablet scan requires fragment authorization control".to_owned(),
            )
        })?;
        checkpoint(&control.control)?;
        let database = self.databases.database(tablet).ok_or_else(|| {
            DistributedError::RemoteTransport(format!("this node does not host tablet {tablet}"))
        })?;
        let principal = self
            .authorization
            .resolve(&database, &control.authorization_context)?;
        let schema = database
            .table(table)
            .map_err(|error| DistributedError::RemoteTransport(error.to_string()))?
            .lock()
            .schema()
            .clone();
        let rows = database
            .query_as_principal_controlled(
                table,
                &mongreldb_core::Query {
                    conditions: Vec::new(),
                    // Fragment scans are an internal streaming input, not a
                    // public final-result window. Capping this at
                    // MAX_FINAL_LIMIT silently dropped every row after
                    // 10,000 before aggregation, sorting, or exchange.
                    limit: None,
                    offset: 0,
                },
                None,
                principal.as_ref(),
                &control.control,
            )
            .map_err(|error| DistributedError::RemoteTransport(error.to_string()))?;
        let batch = crate::arrow_conv::rows_to_batch(&rows, &schema)
            .map_err(|error| DistributedError::Arrow(error.to_string()))?;
        if !include_row_id {
            return Ok(vec![batch]);
        }
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            TOPK_ROWID_COLUMN,
            DataType::UInt64,
            false,
        )));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.row_id.0).collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        Ok(vec![batch])
    }

    fn schema(&self, _table: &str, _tablet: TabletId) -> DistributedResult<Option<SchemaRef>> {
        // `scan` always returns one (possibly empty) batch with the authorized
        // schema, so this fallback is never needed.
        Ok(None)
    }
}

/// In-memory per-`(table, tablet)` record-batch source backing
/// [`InMemoryFragmentExecutor`].
#[derive(Default)]
pub struct InMemoryTableStore {
    tables: parking_lot::RwLock<HashMap<(String, TabletId), Vec<RecordBatch>>>,
    schemas: parking_lot::RwLock<HashMap<String, SchemaRef>>,
}

impl InMemoryTableStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one batch to a tablet of a table, registering the table's
    /// schema from the batch on first sight.
    pub fn insert(&self, table: &str, tablet: TabletId, batch: RecordBatch) {
        self.schemas
            .write()
            .entry(table.to_owned())
            .or_insert_with(|| batch.schema());
        self.tables
            .write()
            .entry((table.to_owned(), tablet))
            .or_default()
            .push(batch);
    }

    /// Registers a table schema explicitly (covers tablets with no rows).
    pub fn register_schema(&self, table: &str, schema: SchemaRef) {
        self.schemas.write().insert(table.to_owned(), schema);
    }

    /// All batches stored for one tablet of a table (empty when none).
    pub fn snapshot(&self, table: &str, tablet: TabletId) -> Vec<RecordBatch> {
        self.tables
            .read()
            .get(&(table.to_owned(), tablet))
            .cloned()
            .unwrap_or_default()
    }

    /// The table's registered schema, when known.
    pub fn schema(&self, table: &str) -> Option<SchemaRef> {
        self.schemas.read().get(table).cloned()
    }
}

impl FragmentTableSource for InMemoryTableStore {
    fn scan(
        &self,
        table: &str,
        tablet: TabletId,
        _include_row_id: bool,
        _control: Option<&FragmentControl>,
    ) -> DistributedResult<Vec<RecordBatch>> {
        Ok(self.snapshot(table, tablet))
    }

    fn schema(&self, table: &str, _tablet: TabletId) -> DistributedResult<Option<SchemaRef>> {
        Ok(self.schema(table))
    }
}

/// Reference [`FragmentExecutor`] over an [`InMemoryTableStore`]. Interprets
/// scans, projections, partial aggregates, local merge sorts, bounded local
/// top-ks (with exact tie information), and limits for real; exchange
/// sources drain their input streams; join operators reject (their execution
/// binding lands with the tablet wave — plan shape is fully tested).
pub struct InMemoryFragmentExecutor {
    store: Arc<dyn FragmentTableSource>,
    /// Wire batch for the local top-k: at most this many rows are emitted
    /// before reporting a bound (`None` = emit `k`, which never needs a
    /// refill). Smaller values exercise the coordinator's adaptive refill.
    topk_emit_batch: Option<usize>,
}

impl InMemoryFragmentExecutor {
    /// An executor over `store` that emits up to `k` top-k rows locally.
    pub fn new(store: Arc<InMemoryTableStore>) -> Self {
        Self {
            store,
            topk_emit_batch: None,
        }
    }

    /// Bounds the local top-k emission batch (refill exerciser).
    pub fn with_topk_emit_batch(store: Arc<InMemoryTableStore>, batch: usize) -> Self {
        Self {
            store,
            topk_emit_batch: Some(batch),
        }
    }

    /// Uses an arbitrary authorized tablet source with the same operator
    /// engine as the deterministic in-memory reference.
    pub fn from_source(store: Arc<dyn FragmentTableSource>) -> Self {
        Self {
            store,
            topk_emit_batch: None,
        }
    }

    /// Replays the scan (+ projection) of a fragment from the store.
    fn scan_batches(
        &self,
        fragment: &PlanFragment,
        control: Option<&FragmentControl>,
    ) -> DistributedResult<Vec<RecordBatch>> {
        let tablet = tablet_of(fragment)?;
        let scan = fragment
            .operators
            .iter()
            .find_map(|operator| match operator {
                FragmentOperator::TabletScan {
                    table,
                    predicate,
                    projection,
                } => Some((table, predicate, projection)),
                _ => None,
            })
            .ok_or_else(|| {
                DistributedError::InvalidPlan(format!(
                    "fragment {} has no tablet scan",
                    fragment.fragment_id
                ))
            })?;
        if scan.1.is_some() {
            return Err(DistributedError::Unsupported(
                "tablet predicate execution is not bound to the fragment operator engine"
                    .to_owned(),
            ));
        }
        let include_row_id = fragment
            .operators
            .iter()
            .any(|operator| matches!(operator, FragmentOperator::DistributedTopK { .. }));
        let mut batches = self.store.scan(scan.0, tablet, include_row_id, control)?;
        if batches.is_empty() {
            if let Some(schema) = self.store.schema(scan.0, tablet)? {
                batches = vec![RecordBatch::new_empty(schema)];
            }
        }
        if !scan.2.is_empty() {
            batches = project_batches(&batches, scan.2)?;
        }
        Ok(batches)
    }
}

#[async_trait::async_trait]
impl FragmentExecutor for InMemoryFragmentExecutor {
    async fn execute(
        &self,
        fragment: &PlanFragment,
        inputs: Vec<FragmentStream>,
        control: FragmentControl,
    ) -> DistributedResult<FragmentStream> {
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut inputs = inputs.into_iter();
        let mut bound = ScoreBound::Unknown;
        for operator in &fragment.operators {
            checkpoint(&control.control)?;
            match operator {
                FragmentOperator::TabletScan { .. } => {
                    batches = self.scan_batches(fragment, Some(&control))?;
                }
                FragmentOperator::RemoteExchangeSource { .. } => {
                    let input = inputs.next().ok_or_else(|| {
                        DistributedError::InvalidPlan(format!(
                            "fragment {} is missing an exchange input stream",
                            fragment.fragment_id
                        ))
                    })?;
                    let frames = drain_stream(input, &control.control).await?;
                    batches.extend(frames.into_iter().map(|frame| frame.batch));
                }
                FragmentOperator::PartialAggregate {
                    group_by,
                    aggregates,
                } => {
                    batches = vec![partial_aggregate_batches(&batches, group_by, aggregates)?];
                }
                FragmentOperator::FinalAggregate {
                    group_by,
                    aggregates,
                } => {
                    batches = vec![final_aggregate_batches(&batches, group_by, aggregates)?];
                }
                FragmentOperator::MergeSort { keys, limit } => {
                    batches = sort_batches_local(&batches, keys, *limit)?;
                }
                FragmentOperator::DistributedTopK { k, score } => {
                    let tablet = tablet_of(fragment)?;
                    let input = prepare_top_k(&batches, &score.column, tablet)?;
                    let emit = self.topk_emit_batch.unwrap_or(*k).min(*k);
                    let (_rows, payload, next) = input.emit(0, emit);
                    batches = vec![payload];
                    bound = match next {
                        Some(next) => ScoreBound::AtMost(next),
                        None => ScoreBound::Exhausted,
                    };
                }
                FragmentOperator::DistributedLimit { limit } => {
                    batches = limit_batches(&batches, *limit);
                }
                FragmentOperator::RemoteExchangeSink { .. } => {
                    // Routing is the transport/coordinator's job; the sink is
                    // a pass-through here.
                }
                FragmentOperator::DistributedHashJoin { .. }
                | FragmentOperator::BroadcastJoin { .. }
                | FragmentOperator::RepartitionJoin { .. } => {
                    return Err(DistributedError::Unsupported(
                        "join execution binding lands with the tablet wave".to_owned(),
                    ));
                }
            }
        }
        let mut frames: Vec<BatchFrame> = batches.into_iter().map(BatchFrame::data).collect();
        if bound != ScoreBound::Unknown {
            if let Some(last) = frames.last_mut() {
                last.score_bound = bound;
            }
        }
        Ok(Box::pin(stream::iter(frames.into_iter().map(Ok))))
    }

    fn refill_top_k(
        &self,
        fragment: &PlanFragment,
        offset: usize,
        limit: usize,
        control: FragmentControl,
    ) -> DistributedResult<TopKRefill> {
        let tablet = tablet_of(fragment)?;
        let score = fragment
            .operators
            .iter()
            .find_map(|operator| match operator {
                FragmentOperator::DistributedTopK { score, .. } => Some(score.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                DistributedError::InvalidPlan(format!(
                    "fragment {} has no distributed top-k operator",
                    fragment.fragment_id
                ))
            })?;
        let batches = self.scan_batches(fragment, Some(&control))?;
        let input = prepare_top_k(&batches, &score.column, tablet)?;
        let (rows, payload, unseen_bound) = input.emit(offset, limit);
        Ok(TopKRefill {
            rows,
            payload,
            unseen_bound,
        })
    }
}

// ---------------------------------------------------------------------------
// Shared execution helpers and real merge operators
// ---------------------------------------------------------------------------

/// Maps an [`ExecutionControl`] checkpoint onto a distributed cancellation.
fn checkpoint(control: &ExecutionControl) -> DistributedResult<()> {
    control
        .checkpoint()
        .map_err(|_| DistributedError::Cancelled(control.reason()))
}

/// The tablet a fragment is assigned to (errors for coordinator fragments).
fn tablet_of(fragment: &PlanFragment) -> DistributedResult<TabletId> {
    match fragment.assignment {
        FragmentAssignment::Tablet(tablet) => Ok(tablet),
        FragmentAssignment::Coordinator => Err(DistributedError::InvalidPlan(format!(
            "fragment {} operator requires a tablet assignment",
            fragment.fragment_id
        ))),
    }
}

/// Drains a fragment stream with cooperative cancellation.
async fn drain_stream(
    mut stream: FragmentStream,
    control: &ExecutionControl,
) -> DistributedResult<Vec<BatchFrame>> {
    let mut frames = Vec::new();
    while let Some(item) = stream.next().await {
        checkpoint(control)?;
        frames.push(item?);
    }
    Ok(frames)
}

/// Concatenates batches (`None` when the input is empty).
fn concat_all(batches: &[RecordBatch]) -> DistributedResult<Option<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(None);
    };
    if batches.len() == 1 {
        return Ok(Some(first.clone()));
    }
    Ok(Some(concat_batches(&first.schema(), batches)?))
}

/// Projects every batch onto the named columns.
fn project_batches(
    batches: &[RecordBatch],
    projection: &[String],
) -> DistributedResult<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            let indexes: Vec<usize> = projection
                .iter()
                .map(|name| {
                    schema.index_of(name).map_err(|_| {
                        DistributedError::InvalidPlan(format!(
                            "projection column `{name}` not in schema"
                        ))
                    })
                })
                .collect::<DistributedResult<Vec<usize>>>()?;
            Ok(batch.project(&indexes)?)
        })
        .collect()
}

/// Builds a row converter + key column indexes for sort keys.
fn row_converter(
    schema: &Schema,
    keys: &[SortKey],
) -> DistributedResult<(RowConverter, Vec<usize>)> {
    let mut fields = Vec::with_capacity(keys.len());
    let mut indexes = Vec::with_capacity(keys.len());
    for key in keys {
        let index = schema.index_of(&key.column).map_err(|_| {
            DistributedError::InvalidPlan(format!("sort key `{}` not in schema", key.column))
        })?;
        // Groundwork null semantics: nulls sort first under descending keys
        // and last under ascending keys. The DataFusion lowering wave maps
        // explicit NULLS FIRST/LAST clauses onto this.
        let options = arrow::compute::SortOptions {
            descending: key.descending,
            nulls_first: key.descending,
        };
        fields.push(SortField::new_with_options(
            schema.field(index).data_type().clone(),
            options,
        ));
        indexes.push(index);
    }
    Ok((RowConverter::new(fields)?, indexes))
}

/// Gathers `order`ed rows of one batch into chunked output batches.
fn take_rows(batch: &RecordBatch, order: &[usize]) -> DistributedResult<Vec<RecordBatch>> {
    let mut out = Vec::new();
    for chunk in order.chunks(COORDINATOR_OUTPUT_BATCH_ROWS) {
        let indices = UInt32Array::from(
            chunk
                .iter()
                .map(|index| u32::try_from(*index).unwrap_or(u32::MAX))
                .collect::<Vec<u32>>(),
        );
        let mut columns = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            columns.push(take(column, &indices, None)?);
        }
        out.push(RecordBatch::try_new(batch.schema(), columns)?);
    }
    if out.is_empty() {
        out.push(RecordBatch::new_empty(batch.schema()));
    }
    Ok(out)
}

/// Interleaves rows from several same-schema streams in `order`
/// (`(stream, row)` pairs) into chunked output batches.
fn emit_interleaved(
    streams: &[RecordBatch],
    order: &[(usize, usize)],
) -> DistributedResult<Vec<RecordBatch>> {
    let Some(first) = streams.first() else {
        return Ok(Vec::new());
    };
    if order.is_empty() {
        return Ok(vec![RecordBatch::new_empty(first.schema())]);
    }
    let schema = first.schema();
    let mut out = Vec::new();
    for chunk in order.chunks(COORDINATOR_OUTPUT_BATCH_ROWS) {
        let mut columns = Vec::with_capacity(schema.fields().len());
        for column_index in 0..schema.fields().len() {
            let refs: Vec<&dyn Array> = streams
                .iter()
                .map(|batch| batch.column(column_index).as_ref())
                .collect();
            columns.push(interleave(&refs, chunk)?);
        }
        out.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    Ok(out)
}

/// Producer-side local sort: full deterministic sort of the (unsorted)
/// input, optionally truncated to `limit` rows.
fn sort_batches_local(
    batches: &[RecordBatch],
    keys: &[SortKey],
    limit: Option<usize>,
) -> DistributedResult<Vec<RecordBatch>> {
    let Some(batch) = concat_all(batches)? else {
        return Ok(Vec::new());
    };
    if batch.num_rows() == 0 {
        return Ok(vec![RecordBatch::new_empty(batch.schema())]);
    }
    let (converter, indexes) = row_converter(&batch.schema(), keys)?;
    let columns: Vec<ArrayRef> = indexes
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect();
    let rows = converter.convert_columns(&columns)?;
    let mut order: Vec<usize> = (0..batch.num_rows()).collect();
    order.sort_by(|left, right| {
        rows.row(*left)
            .cmp(&rows.row(*right))
            .then_with(|| left.cmp(right))
    });
    if let Some(limit) = limit {
        order.truncate(limit);
    }
    take_rows(&batch, &order)
}

/// Coordinator-side merge sort: a deterministic k-way merge over streams
/// that are each already sorted on `keys` (ties break by stream index, so
/// the result is fully deterministic).
fn merge_sorted_streams(
    streams: &[RecordBatch],
    keys: &[SortKey],
    limit: Option<usize>,
) -> DistributedResult<Vec<RecordBatch>> {
    /// Min-heap entry (via `Reverse`): smallest key, then smallest stream.
    struct MergeItem {
        key: Vec<u8>,
        stream: usize,
    }
    impl PartialEq for MergeItem {
        fn eq(&self, other: &Self) -> bool {
            self.key == other.key && self.stream == other.stream
        }
    }
    impl Eq for MergeItem {}
    impl PartialOrd for MergeItem {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for MergeItem {
        fn cmp(&self, other: &Self) -> Ordering {
            self.key
                .cmp(&other.key)
                .then_with(|| self.stream.cmp(&other.stream))
        }
    }

    let mut per_stream: Vec<Vec<Vec<u8>>> = Vec::with_capacity(streams.len());
    for batch in streams {
        let (converter, indexes) = row_converter(&batch.schema(), keys)?;
        let columns: Vec<ArrayRef> = indexes
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect();
        let rows = converter.convert_columns(&columns)?;
        per_stream.push(
            (0..batch.num_rows())
                .map(|row| rows.row(row).as_ref().to_vec())
                .collect(),
        );
    }
    let mut cursors: Vec<usize> = vec![0; streams.len()];
    let mut heap = BinaryHeap::new();
    for (stream, keys) in per_stream.iter().enumerate() {
        if let Some(key) = keys.first() {
            heap.push(std::cmp::Reverse(MergeItem {
                key: key.clone(),
                stream,
            }));
        }
    }
    let mut order = Vec::new();
    while let Some(std::cmp::Reverse(item)) = heap.pop() {
        let row = cursors[item.stream];
        order.push((item.stream, row));
        if limit.is_some_and(|limit| order.len() >= limit) {
            break;
        }
        cursors[item.stream] += 1;
        let cursor = cursors[item.stream];
        if let Some(key) = per_stream[item.stream].get(cursor) {
            heap.push(std::cmp::Reverse(MergeItem {
                key: key.clone(),
                stream: item.stream,
            }));
        }
    }
    emit_interleaved(streams, &order)
}

/// Truncates batches to `limit` rows, preserving stream order.
fn limit_batches(batches: &[RecordBatch], limit: usize) -> Vec<RecordBatch> {
    let mut remaining = limit;
    let mut out = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let rows = batch.num_rows().min(remaining);
        if rows == 0 {
            continue;
        }
        out.push(if rows == batch.num_rows() {
            batch.clone()
        } else {
            batch.slice(0, rows)
        });
        remaining -= rows;
    }
    out
}

/// Routes a producer's output across the sibling consumers of a
/// hash-repartition boundary: rows whose FNV-1a key hash modulo `width`
/// equals `index`.
fn repartition_frames(
    frames: &[BatchFrame],
    keys: &[String],
    width: usize,
    index: usize,
) -> DistributedResult<Vec<BatchFrame>> {
    let batches: Vec<RecordBatch> = frames.iter().map(|frame| frame.batch.clone()).collect();
    let Some(batch) = concat_all(&batches)? else {
        return Ok(Vec::new());
    };
    if batch.num_rows() == 0 || width <= 1 {
        return Ok(vec![BatchFrame::data(batch)]);
    }
    let schema = batch.schema();
    let mut key_columns = Vec::with_capacity(keys.len());
    for key in keys {
        let column_index = schema.index_of(key).map_err(|_| {
            DistributedError::InvalidPlan(format!("repartition key `{key}` not in schema"))
        })?;
        key_columns.push(batch.column(column_index).clone());
    }
    let converter = RowConverter::new(
        key_columns
            .iter()
            .map(|column| SortField::new(column.data_type().clone()))
            .collect::<Vec<SortField>>(),
    )?;
    let rows = converter.convert_columns(&key_columns)?;
    let mut order = Vec::new();
    for row in 0..batch.num_rows() {
        let bucket = (fnv1a64(rows.row(row).as_ref()) % width as u64) as usize;
        if bucket == index {
            order.push(row);
        }
    }
    Ok(take_rows(&batch, &order)?
        .into_iter()
        .map(BatchFrame::data)
        .collect())
}

/// Maps one numeric score cell onto an order-preserving `u64` key (higher
/// sorts better). Null scores map to the minimum key.
fn score_key(array: &dyn Array, row: usize) -> DistributedResult<u64> {
    if array.is_null(row) {
        return Ok(0);
    }
    match array.data_type() {
        DataType::UInt64 => Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("type checked")
            .value(row)),
        DataType::Int64 => {
            let value = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("type checked")
                .value(row);
            Ok((value as u64) ^ (1_u64 << 63))
        }
        DataType::Float64 => {
            let bits = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("type checked")
                .value(row)
                .to_bits();
            Ok(if bits & (1_u64 << 63) != 0 {
                !bits
            } else {
                bits | (1_u64 << 63)
            })
        }
        other => Err(DistributedError::Unsupported(format!(
            "top-k score column of type {other} is not supported in this wave"
        ))),
    }
}

/// Reads the `__rowid` column of a top-k batch.
fn row_ids(batch: &RecordBatch) -> DistributedResult<UInt64Array> {
    let index = batch.schema().index_of(TOPK_ROWID_COLUMN).map_err(|_| {
        DistributedError::InvalidPlan(format!(
            "top-k stream is missing the `{TOPK_ROWID_COLUMN}` column"
        ))
    })?;
    let array = batch.column(index);
    if array.data_type() != &DataType::UInt64 {
        return Err(DistributedError::InvalidPlan(format!(
            "`{TOPK_ROWID_COLUMN}` must be UInt64, found {}",
            array.data_type()
        )));
    }
    Ok(array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("type checked")
        .clone())
}

/// A top-k input fully prepared for bounded emission: the concatenated
/// payload plus the globally sorted candidate list.
struct TopKInput {
    batch: RecordBatch,
    /// Candidates, best first, aligned with `positions`.
    candidates: Vec<TopKCandidate>,
    /// Row index in `batch` of each candidate.
    positions: Vec<usize>,
}

impl TopKInput {
    /// Emits candidates `[offset, offset + limit)`: the candidates
    /// themselves, their payload rows, and the tightened unseen bound
    /// (`None` when nothing remains).
    fn emit(&self, offset: usize, limit: usize) -> (Vec<TopKCandidate>, RecordBatch, Option<u64>) {
        let offset = offset.min(self.candidates.len());
        let end = (offset + limit).min(self.candidates.len());
        let rows = self.candidates[offset..end].to_vec();
        let positions = &self.positions[offset..end];
        let payload = take_rows(&self.batch, positions)
            .unwrap_or_else(|_| vec![RecordBatch::new_empty(self.batch.schema())])
            .into_iter()
            .next()
            .unwrap_or_else(|| RecordBatch::new_empty(self.batch.schema()));
        let bound = self.candidates.get(end).map(|candidate| candidate.score);
        (rows, payload, bound)
    }
}

/// Builds the sorted candidate list of a top-k input.
fn prepare_top_k(
    batches: &[RecordBatch],
    score_column: &str,
    tablet: TabletId,
) -> DistributedResult<TopKInput> {
    let Some(batch) = concat_all(batches)? else {
        return Err(DistributedError::InvalidPlan(
            "top-k input has no batches".to_owned(),
        ));
    };
    let schema = batch.schema();
    let score_index = schema.index_of(score_column).map_err(|_| {
        DistributedError::InvalidPlan(format!("top-k score column `{score_column}` not in schema"))
    })?;
    let score_array = batch.column(score_index).clone();
    let row_id_array = row_ids(&batch)?;
    let mut candidates = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        candidates.push(TopKCandidate {
            score: score_key(score_array.as_ref(), row)?,
            tablet,
            row_id: RowId(row_id_array.value(row)),
        });
    }
    let mut positions: Vec<usize> = (0..candidates.len()).collect();
    positions.sort_by(|left, right| topk_cmp(&candidates[*left], &candidates[*right]));
    let candidates = positions.iter().map(|index| candidates[*index]).collect();
    Ok(TopKInput {
        batch,
        candidates,
        positions,
    })
}

// ---------------------------------------------------------------------------
// Aggregation combine (partial + final)
// ---------------------------------------------------------------------------

/// One numeric cell (the groundwork supports Int64 and Float64 aggregate
/// inputs).
#[derive(Clone, Copy, Debug)]
enum AggValue {
    I64(i64),
    F64(f64),
}

/// Reads one numeric cell (`None` for nulls).
fn numeric_cell(array: &dyn Array, row: usize) -> DistributedResult<Option<AggValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int64 => Ok(Some(AggValue::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("type checked")
                .value(row),
        ))),
        DataType::Float64 => Ok(Some(AggValue::F64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("type checked")
                .value(row),
        ))),
        other => Err(DistributedError::Unsupported(format!(
            "aggregate over {other} is not supported in this wave"
        ))),
    }
}

/// Per-group accumulator for one aggregate expression.
#[derive(Clone, Debug)]
enum AggAccum {
    Count(i64),
    SumI(i128, bool),
    SumF(f64, bool),
    MinI(i64, bool),
    MinF(f64, bool),
    MaxI(i64, bool),
    MaxF(f64, bool),
    Avg { sum: f64, count: i64 },
}

impl AggAccum {
    /// A fresh accumulator for `function` over a value column of type
    /// `float` (`true` = Float64, `false` = Int64).
    fn fresh(function: AggregateFunction, float: bool) -> Self {
        match function {
            AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum if float => Self::SumF(0.0, false),
            AggregateFunction::Sum => Self::SumI(0, false),
            AggregateFunction::Min if float => Self::MinF(f64::INFINITY, false),
            AggregateFunction::Min => Self::MinI(i64::MAX, false),
            AggregateFunction::Max if float => Self::MaxF(f64::NEG_INFINITY, false),
            AggregateFunction::Max => Self::MaxI(i64::MIN, false),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    /// Folds one value cell (shared by partial folds and final combines).
    fn fold_value(&mut self, cell: Option<AggValue>) {
        match (self, cell) {
            (Self::SumI(sum, seen), Some(AggValue::I64(value))) => {
                *sum += i128::from(value);
                *seen = true;
            }
            (Self::SumF(sum, seen), Some(AggValue::F64(value))) => {
                *sum += value;
                *seen = true;
            }
            (Self::MinI(min, seen), Some(AggValue::I64(value))) => {
                *min = (*min).min(value);
                *seen = true;
            }
            (Self::MinF(min, seen), Some(AggValue::F64(value))) => {
                *min = min.min(value);
                *seen = true;
            }
            (Self::MaxI(max, seen), Some(AggValue::I64(value))) => {
                *max = (*max).max(value);
                *seen = true;
            }
            (Self::MaxF(max, seen), Some(AggValue::F64(value))) => {
                *max = max.max(value);
                *seen = true;
            }
            (Self::Avg { sum, count }, Some(AggValue::I64(value))) => {
                *sum += value as f64;
                *count += 1;
            }
            (Self::Avg { sum, count }, Some(AggValue::F64(value))) => {
                *sum += value;
                *count += 1;
            }
            _ => {}
        }
    }
}

/// One folded group.
struct GroupEntry {
    /// Global row (in the concatenated input) whose key columns represent
    /// this group in the output.
    first_row: usize,
    accums: Vec<AggAccum>,
}

/// Groups in first-seen order (deterministic for a fixed input order).
#[derive(Default)]
struct GroupFold {
    order: Vec<String>,
    groups: HashMap<String, GroupEntry>,
}

impl GroupFold {
    fn entry(&mut self, key: String, row: usize, templates: &[AggAccum]) -> &mut GroupEntry {
        if !self.groups.contains_key(&key) {
            self.order.push(key.clone());
            self.groups.insert(
                key.clone(),
                GroupEntry {
                    first_row: row,
                    accums: templates.to_vec(),
                },
            );
        }
        self.groups.get_mut(&key).expect("inserted above")
    }
}

/// The deterministic group key of one row (display form of the key columns).
fn group_key(batch: &RecordBatch, key_indexes: &[usize], row: usize) -> DistributedResult<String> {
    let mut parts = Vec::with_capacity(key_indexes.len());
    for index in key_indexes {
        parts.push(array_value_to_string(batch.column(*index).as_ref(), row)?);
    }
    Ok(parts.join("\u{1f}"))
}

/// Resolves group-by column indexes.
fn resolve_columns(schema: &Schema, columns: &[String]) -> DistributedResult<Vec<usize>> {
    columns
        .iter()
        .map(|column| {
            schema.index_of(column).map_err(|_| {
                DistributedError::InvalidPlan(format!("group-by column `{column}` not in schema"))
            })
        })
        .collect()
}

/// The value-column type marker of one aggregate (`true` = Float64).
fn aggregate_float(schema: &Schema, aggregate: &AggregateExpr) -> DistributedResult<bool> {
    match &aggregate.column {
        None => Ok(false),
        Some(column) => {
            let index = schema.index_of(column).map_err(|_| {
                DistributedError::InvalidPlan(format!("aggregate column `{column}` not in schema"))
            })?;
            match schema.field(index).data_type() {
                DataType::Int64 => Ok(false),
                DataType::Float64 => Ok(true),
                other => Err(DistributedError::Unsupported(format!(
                    "aggregate over {other} is not supported in this wave"
                ))),
            }
        }
    }
}

/// Gathers group key columns at each group's first-seen row.
fn take_group_keys(
    batch: &RecordBatch,
    key_indexes: &[usize],
    fold: &GroupFold,
) -> DistributedResult<Vec<ArrayRef>> {
    if key_indexes.is_empty() {
        return Ok(Vec::new());
    }
    let indices = UInt32Array::from(
        fold.order
            .iter()
            .map(|key| u32::try_from(fold.groups[key].first_row).unwrap_or(u32::MAX))
            .collect::<Vec<u32>>(),
    );
    let mut columns = Vec::with_capacity(key_indexes.len());
    for index in key_indexes {
        columns.push(take(batch.column(*index), &indices, None)?);
    }
    Ok(columns)
}

/// Emits one integer accumulator column (partial or final).
fn emit_int_accum(groups: &GroupFold, index: usize) -> DistributedResult<Vec<Option<i64>>> {
    let mut values = Vec::with_capacity(groups.order.len());
    for key in &groups.order {
        let accum = &groups.groups[key].accums[index];
        values.push(match accum {
            AggAccum::Count(count) => Some(*count),
            AggAccum::SumI(sum, seen) => seen
                .then(|| {
                    i64::try_from(*sum).map_err(|_| {
                        DistributedError::Unsupported(
                            "integer sum overflow in this wave".to_owned(),
                        )
                    })
                })
                .transpose()?,
            AggAccum::MinI(value, seen) | AggAccum::MaxI(value, seen) => seen.then_some(*value),
            other => {
                return Err(DistributedError::InvalidPlan(format!(
                    "internal: accumulator {other:?} is not an integer column"
                )))
            }
        });
    }
    Ok(values)
}

/// Emits one float accumulator column (partial or final).
fn emit_float_accum(groups: &GroupFold, index: usize) -> DistributedResult<Vec<Option<f64>>> {
    let mut values = Vec::with_capacity(groups.order.len());
    for key in &groups.order {
        let accum = &groups.groups[key].accums[index];
        values.push(match accum {
            AggAccum::SumF(sum, seen) => seen.then_some(*sum),
            AggAccum::MinF(value, seen) | AggAccum::MaxF(value, seen) => seen.then_some(*value),
            other => {
                return Err(DistributedError::InvalidPlan(format!(
                    "internal: accumulator {other:?} is not a float column"
                )))
            }
        });
    }
    Ok(values)
}

/// Per-tablet partial aggregation (the producer half of the two-phase
/// aggregate, spec section 12.10).
fn partial_aggregate_batches(
    batches: &[RecordBatch],
    group_by: &[String],
    aggregates: &[AggregateExpr],
) -> DistributedResult<RecordBatch> {
    let Some(batch) = concat_all(batches)? else {
        return Err(DistributedError::InvalidPlan(
            "aggregate input has no batches".to_owned(),
        ));
    };
    let schema = batch.schema();
    let key_indexes = resolve_columns(&schema, group_by)?;
    let mut value_indexes = Vec::with_capacity(aggregates.len());
    let mut templates = Vec::with_capacity(aggregates.len());
    for aggregate in aggregates {
        let float = aggregate_float(&schema, aggregate)?;
        value_indexes.push(match &aggregate.column {
            Some(column) => Some(schema.index_of(column).map_err(|_| {
                DistributedError::InvalidPlan(format!("aggregate column `{column}` not in schema"))
            })?),
            None => None,
        });
        templates.push(AggAccum::fresh(aggregate.function, float));
    }
    let mut fold = GroupFold::default();
    for row in 0..batch.num_rows() {
        let key = group_key(&batch, &key_indexes, row)?;
        let entry = fold.entry(key, row, &templates);
        for (index, aggregate) in aggregates.iter().enumerate() {
            match aggregate.function {
                AggregateFunction::Count => {
                    let counted = match value_indexes[index] {
                        Some(column) => !batch.column(column).is_null(row),
                        None => true,
                    };
                    if counted {
                        let AggAccum::Count(count) = &mut entry.accums[index] else {
                            unreachable!("count template");
                        };
                        *count += 1;
                    }
                }
                _ => {
                    let cell = match value_indexes[index] {
                        Some(column) => numeric_cell(batch.column(column).as_ref(), row)?,
                        None => None,
                    };
                    entry.accums[index].fold_value(cell);
                }
            }
        }
    }
    // Emit: key columns at first-seen rows, then partial columns.
    let mut columns = take_group_keys(&batch, &key_indexes, &fold)?;
    let mut fields: Vec<Field> = key_indexes
        .iter()
        .map(|index| schema.field(*index).clone())
        .collect();
    for (index, aggregate) in aggregates.iter().enumerate() {
        match aggregate.function {
            AggregateFunction::Avg => {
                let mut sums = Vec::with_capacity(fold.order.len());
                let mut counts = Vec::with_capacity(fold.order.len());
                for key in &fold.order {
                    let AggAccum::Avg { sum, count } = &fold.groups[key].accums[index] else {
                        unreachable!("avg template");
                    };
                    sums.push(Some(*sum));
                    counts.push(Some(*count));
                }
                fields.push(Field::new(
                    format!("__partial_{index}_sum"),
                    DataType::Float64,
                    true,
                ));
                columns.push(Arc::new(Float64Array::from(sums)));
                fields.push(Field::new(
                    format!("__partial_{index}_count"),
                    DataType::Int64,
                    true,
                ));
                columns.push(Arc::new(Int64Array::from(counts)));
            }
            AggregateFunction::Count => {
                fields.push(Field::new(
                    format!("__partial_{index}"),
                    DataType::Int64,
                    true,
                ));
                columns.push(Arc::new(Int64Array::from(emit_int_accum(&fold, index)?)));
            }
            _ => match &templates[index] {
                AggAccum::SumI(..) | AggAccum::MinI(..) | AggAccum::MaxI(..) => {
                    fields.push(Field::new(
                        format!("__partial_{index}"),
                        DataType::Int64,
                        true,
                    ));
                    columns.push(Arc::new(Int64Array::from(emit_int_accum(&fold, index)?)));
                }
                AggAccum::SumF(..) | AggAccum::MinF(..) | AggAccum::MaxF(..) => {
                    fields.push(Field::new(
                        format!("__partial_{index}"),
                        DataType::Float64,
                        true,
                    ));
                    columns.push(Arc::new(Float64Array::from(emit_float_accum(
                        &fold, index,
                    )?)));
                }
                other => {
                    return Err(DistributedError::InvalidPlan(format!(
                        "internal: unexpected accumulator {other:?}"
                    )))
                }
            },
        }
    }
    Ok(RecordBatch::try_new(Schema::new(fields).into(), columns)?)
}

/// Coordinator-side combine of partial aggregates into final results (spec
/// section 12.10).
fn final_aggregate_batches(
    batches: &[RecordBatch],
    group_by: &[String],
    aggregates: &[AggregateExpr],
) -> DistributedResult<RecordBatch> {
    let Some(batch) = concat_all(batches)? else {
        return Err(DistributedError::InvalidPlan(
            "aggregate input has no batches".to_owned(),
        ));
    };
    let schema = batch.schema();
    let key_indexes = resolve_columns(&schema, group_by)?;
    // Resolve the partial columns produced by `partial_aggregate_batches`.
    let mut partial_indexes: Vec<(usize, Option<usize>)> = Vec::with_capacity(aggregates.len());
    let mut templates = Vec::with_capacity(aggregates.len());
    for (index, aggregate) in aggregates.iter().enumerate() {
        let value_name = if aggregate.function == AggregateFunction::Avg {
            format!("__partial_{index}_sum")
        } else {
            format!("__partial_{index}")
        };
        let value_index = schema.index_of(&value_name).map_err(|_| {
            DistributedError::InvalidPlan(format!(
                "partial aggregate column `{value_name}` not in schema"
            ))
        })?;
        let float = match schema.field(value_index).data_type() {
            DataType::Int64 => false,
            DataType::Float64 => true,
            other => {
                return Err(DistributedError::Unsupported(format!(
                    "aggregate partial of type {other} is not supported in this wave"
                )))
            }
        };
        templates.push(AggAccum::fresh(aggregate.function, float));
        let count_index = if aggregate.function == AggregateFunction::Avg {
            let count_name = format!("__partial_{index}_count");
            Some(schema.index_of(&count_name).map_err(|_| {
                DistributedError::InvalidPlan(format!(
                    "partial aggregate column `{count_name}` not in schema"
                ))
            })?)
        } else {
            None
        };
        partial_indexes.push((value_index, count_index));
    }
    let mut fold = GroupFold::default();
    for row in 0..batch.num_rows() {
        let key = group_key(&batch, &key_indexes, row)?;
        let entry = fold.entry(key, row, &templates);
        for (index, aggregate) in aggregates.iter().enumerate() {
            let (value_index, count_index) = partial_indexes[index];
            match aggregate.function {
                AggregateFunction::Count => {
                    if let Some(AggValue::I64(value)) =
                        numeric_cell(batch.column(value_index).as_ref(), row)?
                    {
                        let AggAccum::Count(count) = &mut entry.accums[index] else {
                            unreachable!("count template");
                        };
                        *count += value;
                    }
                }
                AggregateFunction::Avg => {
                    let sum = numeric_cell(batch.column(value_index).as_ref(), row)?;
                    let count = match count_index {
                        Some(count_index) => numeric_cell(batch.column(count_index).as_ref(), row)?,
                        None => None,
                    };
                    let AggAccum::Avg {
                        sum: total,
                        count: rows,
                    } = &mut entry.accums[index]
                    else {
                        unreachable!("avg template");
                    };
                    match sum {
                        Some(AggValue::F64(value)) => *total += value,
                        Some(AggValue::I64(value)) => *total += value as f64,
                        None => {}
                    }
                    if let Some(AggValue::I64(value)) = count {
                        *rows += value;
                    }
                }
                _ => {
                    let cell = numeric_cell(batch.column(value_index).as_ref(), row)?;
                    entry.accums[index].fold_value(cell);
                }
            }
        }
    }
    // SQL semantics: an empty input with no group-by still yields one row.
    if fold.order.is_empty() && group_by.is_empty() {
        fold.entry(String::new(), 0, &templates);
    }
    // Emit: key columns at first-seen rows, then one column per aggregate.
    let mut columns = take_group_keys(&batch, &key_indexes, &fold)?;
    let mut fields: Vec<Field> = key_indexes
        .iter()
        .map(|index| schema.field(*index).clone())
        .collect();
    for (index, aggregate) in aggregates.iter().enumerate() {
        let name = aggregate_output_name(aggregate);
        match &templates[index] {
            AggAccum::Count(_) | AggAccum::SumI(..) | AggAccum::MinI(..) | AggAccum::MaxI(..) => {
                fields.push(Field::new(name, DataType::Int64, true));
                columns.push(Arc::new(Int64Array::from(emit_int_accum(&fold, index)?)));
            }
            AggAccum::SumF(..) | AggAccum::MinF(..) | AggAccum::MaxF(..) => {
                fields.push(Field::new(name, DataType::Float64, true));
                columns.push(Arc::new(Float64Array::from(emit_float_accum(
                    &fold, index,
                )?)));
            }
            AggAccum::Avg { .. } => {
                let values: Vec<Option<f64>> = fold
                    .order
                    .iter()
                    .map(|key| match &fold.groups[key].accums[index] {
                        AggAccum::Avg { sum, count } => {
                            (*count > 0).then_some(*sum / *count as f64)
                        }
                        _ => unreachable!("avg template"),
                    })
                    .collect();
                fields.push(Field::new(name, DataType::Float64, true));
                columns.push(Arc::new(Float64Array::from(values)));
            }
        }
    }
    Ok(RecordBatch::try_new(Schema::new(fields).into(), columns)?)
}

// ---------------------------------------------------------------------------
// Coordinator runtime (spec section 12.10)
// ---------------------------------------------------------------------------

/// Per-query fragment resource ledger. Reservations are RAII: dropping a
/// [`ResourcePermit`] releases its share.
#[derive(Debug)]
pub struct ResourceLedger {
    state: Mutex<LedgerState>,
    max_fragments: usize,
    max_bytes: u64,
}

#[derive(Default, Debug)]
struct LedgerState {
    fragments: usize,
    bytes: u64,
}

/// A held resource reservation; released on drop.
#[derive(Debug)]
pub struct ResourcePermit {
    ledger: Arc<ResourceLedger>,
    bytes: u64,
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        let mut state = self.ledger.state.lock();
        state.fragments = state.fragments.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(self.bytes);
    }
}

impl ResourceLedger {
    /// A ledger admitting at most `max_fragments` concurrent fragments and
    /// `max_bytes` total estimated bytes.
    pub fn new(max_fragments: usize, max_bytes: u64) -> Self {
        Self {
            state: Mutex::new(LedgerState::default()),
            max_fragments,
            max_bytes,
        }
    }

    /// Reserves one fragment's estimated resources (spec section 12.10:
    /// "workers reserve resources").
    pub fn reserve(self: &Arc<Self>, fragment: &PlanFragment) -> DistributedResult<ResourcePermit> {
        let mut state = self.state.lock();
        if state.fragments >= self.max_fragments {
            return Err(DistributedError::Reservation {
                fragment_id: fragment.fragment_id,
                reason: format!("fragment concurrency limit {} reached", self.max_fragments),
            });
        }
        if state.bytes.saturating_add(fragment.estimated_bytes) > self.max_bytes {
            return Err(DistributedError::Reservation {
                fragment_id: fragment.fragment_id,
                reason: format!(
                    "estimated bytes {} exceed the {} byte budget",
                    state.bytes.saturating_add(fragment.estimated_bytes),
                    self.max_bytes
                ),
            });
        }
        state.fragments += 1;
        state.bytes = state.bytes.saturating_add(fragment.estimated_bytes);
        Ok(ResourcePermit {
            ledger: Arc::clone(self),
            bytes: fragment.estimated_bytes,
        })
    }

    /// Currently reserved fragment count.
    pub fn reserved_fragments(&self) -> usize {
        self.state.lock().fragments
    }

    /// Currently reserved estimated bytes.
    pub fn reserved_bytes(&self) -> u64 {
        self.state.lock().bytes
    }
}

/// Worker lease ledger (spec section 12.10: "worker lease expiry cleans
/// abandoned fragments"). Workers are keyed by the tablet whose data they
/// serve this wave; the node-level binding lands with the transport wave.
#[derive(Default)]
pub struct LeaseLedger {
    leases: Mutex<HashMap<TabletId, Instant>>,
}

impl LeaseLedger {
    /// Renews a worker's lease to `expiry`.
    pub fn renew(&self, worker: TabletId, expiry: Instant) {
        self.leases.lock().insert(worker, expiry);
    }

    /// A worker's current lease expiry.
    pub fn expiry(&self, worker: &TabletId) -> Option<Instant> {
        self.leases.lock().get(worker).copied()
    }

    /// Removes and returns every worker whose lease expired at or before
    /// `now`.
    pub fn sweep(&self, now: Instant) -> Vec<TabletId> {
        let mut leases = self.leases.lock();
        let expired: Vec<TabletId> = leases
            .iter()
            .filter(|(_, expiry)| **expiry <= now)
            .map(|(worker, _)| *worker)
            .collect();
        for worker in &expired {
            leases.remove(worker);
        }
        expired
    }
}

/// One in-flight fragment of a running query.
struct InFlight {
    worker: Option<TabletId>,
    control: ExecutionControl,
    _permit: ResourcePermit,
}

/// Per-query execution state tracked by the coordinator.
struct ExecutionState {
    query_id: QueryId,
    in_flight: Mutex<HashMap<FragmentId, InFlight>>,
}

impl ExecutionState {
    fn new(query_id: QueryId) -> Self {
        Self {
            query_id,
            in_flight: Mutex::new(HashMap::new()),
        }
    }
}

/// One producer stream feeding a coordinator (root) fragment.
struct ProducerInput {
    fragment_id: FragmentId,
    tablet: Option<TabletId>,
    frames: Vec<BatchFrame>,
}

/// The root fragment's working data: either per-producer streams (fresh from
/// the exchange edges) or already-combined batches (after a coordinator
/// operator ran).
enum RootData {
    Streams(Vec<ProducerInput>),
    Batches(Vec<RecordBatch>),
}

impl RootData {
    /// All payload batches, in stream order.
    fn flatten(&self) -> Vec<RecordBatch> {
        match self {
            Self::Streams(inputs) => inputs
                .iter()
                .flat_map(|input| input.frames.iter().map(|frame| frame.batch.clone()))
                .collect(),
            Self::Batches(batches) => batches.clone(),
        }
    }
}

/// The query coordinator (spec section 12.10): registers the query with the
/// existing [`SqlQueryRegistry`] (so `registry.cancel(...)` reaches every
/// fragment through the [`ExecutionControl`] hierarchy), reserves resources
/// per fragment, fans out cancellation to every fragment, sweeps expired
/// worker leases to clean abandoned fragments, and merges producer streams
/// per the exchange descriptors with the real in-memory operators.
pub struct Coordinator {
    transport: Arc<dyn FragmentTransport>,
    registry: Arc<SqlQueryRegistry>,
    resources: Arc<ResourceLedger>,
    leases: LeaseLedger,
    lease_ttl: Duration,
    executions: Mutex<HashMap<QueryId, Arc<ExecutionState>>>,
}

impl Coordinator {
    /// A coordinator with default limits (1024 concurrent fragments, 16 GiB
    /// of estimated bytes, 30 second worker leases).
    pub fn new(transport: Arc<dyn FragmentTransport>, registry: Arc<SqlQueryRegistry>) -> Self {
        Self::with_limits(
            transport,
            registry,
            1_024,
            16 * 1024 * 1024 * 1024,
            Duration::from_secs(30),
        )
    }

    /// A coordinator with explicit resource limits and lease TTL.
    pub fn with_limits(
        transport: Arc<dyn FragmentTransport>,
        registry: Arc<SqlQueryRegistry>,
        max_fragments: usize,
        max_bytes: u64,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            transport,
            registry,
            resources: Arc::new(ResourceLedger::new(max_fragments, max_bytes)),
            leases: LeaseLedger::default(),
            lease_ttl,
            executions: Mutex::new(HashMap::new()),
        }
    }

    /// The fragment resource ledger (test introspection).
    pub fn resources(&self) -> &Arc<ResourceLedger> {
        &self.resources
    }

    /// Executes a distributed plan to completion, returning the root
    /// fragment's output batches.
    ///
    /// The plan's [`QueryId`] is registered with the query registry first;
    /// every fragment runs under a child [`ExecutionControl`] of that
    /// registration, so a registry-level cancel fans out to all fragments.
    /// Fragments execute layer by layer (producers before consumers),
    /// concurrently within a layer. The coordinator-local root fragment's
    /// merge operators ([`FragmentOperator::MergeSort`],
    /// [`FragmentOperator::FinalAggregate`],
    /// [`FragmentOperator::DistributedTopK`],
    /// [`FragmentOperator::DistributedLimit`]) run over the producer streams
    /// for real.
    pub async fn execute(&self, plan: &DistributedPlan) -> DistributedResult<Vec<RecordBatch>> {
        self.execute_with_authorization(plan, &[]).await
    }

    /// Executes a plan while forwarding a bounded, server-issued
    /// authorization envelope to every tablet worker.
    pub async fn execute_with_authorization(
        &self,
        plan: &DistributedPlan,
        authorization_context: &[u8],
    ) -> DistributedResult<Vec<RecordBatch>> {
        if authorization_context.len() > MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES {
            return Err(DistributedError::InvalidPlan(format!(
                "fragment authorization context is {} bytes; limit is {}",
                authorization_context.len(),
                MAX_FRAGMENT_AUTHORIZATION_CONTEXT_BYTES
            )));
        }
        let registry_id = to_registry_query_id(&plan.query_id)?;
        let registered = self
            .registry
            .register(SqlQueryOptions {
                query_id: Some(registry_id),
                ..Default::default()
            })
            .map_err(|error| {
                DistributedError::InvalidPlan(format!("query registration failed: {error}"))
            })?;
        let result = self
            .execute_registered(plan, &registered, authorization_context.into())
            .await;
        match &result {
            Ok(_) => {
                let _ = registered.try_complete();
            }
            Err(_) => registered.fail(),
        }
        result
    }

    /// Cancels a running distributed query: registry cancel (which reaches
    /// every fragment control through the parent-child hierarchy) plus an
    /// explicit transport cancel per in-flight fragment (spec section 12.10:
    /// "cancellation fans out to every fragment"). Returns true when the
    /// registry accepted (or already observed) the cancellation.
    pub fn cancel_query(&self, query_id: &QueryId) -> DistributedResult<bool> {
        let outcome = self.registry.cancel(to_registry_query_id(query_id)?);
        let state = self.executions.lock().get(query_id).cloned();
        if let Some(state) = state {
            let in_flight = state.in_flight.lock();
            for (fragment_id, inflight) in in_flight.iter() {
                inflight.control.cancel(CancellationReason::ClientRequest);
                let _ = self.transport.cancel_fragment(*query_id, *fragment_id);
            }
        }
        Ok(matches!(
            outcome,
            CancelOutcome::Accepted | CancelOutcome::AlreadyCancelling
        ))
    }

    /// Sweeps expired worker leases and cleans their abandoned in-flight
    /// fragments: cancels the fragment control (as
    /// [`CancellationReason::ServerShutdown`]), notifies the transport, and
    /// releases the resource reservation (spec section 12.10: "worker lease
    /// expiry cleans abandoned fragments"). Returns the number of fragments
    /// cleaned.
    pub fn sweep_expired_leases(&self, now: Instant) -> usize {
        let expired = self.leases.sweep(now);
        if expired.is_empty() {
            return 0;
        }
        let states: Vec<Arc<ExecutionState>> = self.executions.lock().values().cloned().collect();
        let mut cleaned = 0;
        for state in states {
            let victims: Vec<FragmentId> = {
                let in_flight = state.in_flight.lock();
                in_flight
                    .iter()
                    .filter(|(_, inflight)| {
                        inflight
                            .worker
                            .is_some_and(|worker| expired.contains(&worker))
                    })
                    .map(|(fragment_id, _)| *fragment_id)
                    .collect()
            };
            for fragment_id in victims {
                let inflight = state.in_flight.lock().remove(&fragment_id);
                if let Some(inflight) = inflight {
                    inflight.control.cancel(CancellationReason::ServerShutdown);
                    drop(inflight);
                    let _ = self.transport.cancel_fragment(state.query_id, fragment_id);
                    cleaned += 1;
                }
            }
        }
        cleaned
    }

    async fn execute_registered(
        &self,
        plan: &DistributedPlan,
        registered: &RegisteredSqlQuery,
        authorization_context: Arc<[u8]>,
    ) -> DistributedResult<Vec<RecordBatch>> {
        validate_plan_shape(plan)?;
        let root = plan
            .root_fragment_id()
            .ok_or_else(|| DistributedError::InvalidPlan("plan has no root fragment".to_owned()))?;
        let state = Arc::new(ExecutionState::new(plan.query_id));
        self.executions
            .lock()
            .insert(plan.query_id, Arc::clone(&state));
        let result = self
            .run_plan(plan, root, registered, &state, authorization_context)
            .await;
        self.executions.lock().remove(&plan.query_id);
        result
    }

    async fn run_plan(
        &self,
        plan: &DistributedPlan,
        root: FragmentId,
        registered: &RegisteredSqlQuery,
        state: &Arc<ExecutionState>,
        authorization_context: Arc<[u8]>,
    ) -> DistributedResult<Vec<RecordBatch>> {
        let parent = registered.control().clone();
        let layers = fragment_layers(plan, root)?;
        let mut outputs: HashMap<FragmentId, Vec<BatchFrame>> = HashMap::new();
        for layer in &layers {
            checkpoint(&parent)?;
            let now = Instant::now();
            for &fragment_id in layer {
                if let FragmentAssignment::Tablet(tablet) =
                    plan.fragments[fragment_id as usize].assignment
                {
                    self.leases.renew(tablet, now + self.lease_ttl);
                }
            }
            let mut tasks = FuturesUnordered::new();
            let mut spawn_error: Option<DistributedError> = None;
            for &fragment_id in layer {
                let fragment = &plan.fragments[fragment_id as usize];
                let reserved = match self.resources.reserve(fragment) {
                    Ok(permit) => permit,
                    Err(error) => {
                        spawn_error = Some(error);
                        break;
                    }
                };
                let inputs = match build_inputs(plan, fragment_id, &outputs) {
                    Ok(inputs) => inputs,
                    Err(error) => {
                        spawn_error = Some(error);
                        break;
                    }
                };
                let control = parent.child_with_deadline(None);
                let worker = match fragment.assignment {
                    FragmentAssignment::Tablet(tablet) => Some(tablet),
                    FragmentAssignment::Coordinator => None,
                };
                state.in_flight.lock().insert(
                    fragment_id,
                    InFlight {
                        worker,
                        control: control.clone(),
                        _permit: reserved,
                    },
                );
                let fragment = fragment.clone();
                let fragment_control = FragmentControl {
                    control,
                    max_spill_bytes: fragment.max_spill_bytes,
                    authorization_context: Arc::clone(&authorization_context),
                };
                let transport = Arc::clone(&self.transport);
                tasks.push(async move {
                    let worker = worker_label(&fragment);
                    let drain_control = fragment_control.control.clone();
                    let result = async {
                        let stream = transport
                            .execute_fragment(plan.query_id, &fragment, inputs, fragment_control)
                            .await?;
                        drain_stream(stream, &drain_control).await
                    }
                    .await;
                    (fragment.fragment_id, worker, result)
                });
            }
            if let Some(error) = spawn_error {
                self.abort(plan, state);
                while let Some((fragment_id, _, _)) = tasks.next().await {
                    state.in_flight.lock().remove(&fragment_id);
                }
                return Err(error);
            }
            let mut failure: Option<DistributedError> = None;
            while let Some((fragment_id, worker, result)) = tasks.next().await {
                state.in_flight.lock().remove(&fragment_id);
                match result {
                    Ok(frames) => {
                        outputs.insert(fragment_id, frames);
                    }
                    Err(error) => {
                        if failure.is_none() {
                            failure = Some(match error {
                                DistributedError::Cancelled(reason) => {
                                    DistributedError::Cancelled(reason)
                                }
                                other => DistributedError::FragmentExecution {
                                    fragment_id,
                                    worker,
                                    message: other.to_string(),
                                },
                            });
                        }
                    }
                }
            }
            if let Some(error) = failure {
                self.abort(plan, state);
                return Err(error);
            }
        }
        checkpoint(&parent)?;
        self.run_root(plan, root, &outputs, &parent, authorization_context)
            .await
    }

    /// Cancels every in-flight fragment control and notifies the transport
    /// for every fragment (the abort path's cancellation fan-out).
    fn abort(&self, plan: &DistributedPlan, state: &Arc<ExecutionState>) {
        {
            let in_flight = state.in_flight.lock();
            for inflight in in_flight.values() {
                inflight.control.cancel(CancellationReason::ClientRequest);
            }
        }
        for fragment in &plan.fragments {
            let _ = self
                .transport
                .cancel_fragment(plan.query_id, fragment.fragment_id);
        }
    }

    /// Runs the coordinator-local root fragment over the producer outputs.
    async fn run_root(
        &self,
        plan: &DistributedPlan,
        root: FragmentId,
        outputs: &HashMap<FragmentId, Vec<BatchFrame>>,
        parent: &ExecutionControl,
        authorization_context: Arc<[u8]>,
    ) -> DistributedResult<Vec<RecordBatch>> {
        let fragment = &plan.fragments[root as usize];
        let mut inputs: Vec<ProducerInput> = Vec::new();
        for operator in &fragment.operators {
            if let FragmentOperator::RemoteExchangeSource { exchange } = operator {
                let edge = plan.exchanges.get(*exchange as usize).ok_or_else(|| {
                    DistributedError::InvalidPlan(format!("unknown exchange {exchange}"))
                })?;
                let producer = plan.fragments.get(edge.producer as usize).ok_or_else(|| {
                    DistributedError::InvalidPlan(format!(
                        "unknown producer fragment {}",
                        edge.producer
                    ))
                })?;
                let tablet = match producer.assignment {
                    FragmentAssignment::Tablet(tablet) => Some(tablet),
                    FragmentAssignment::Coordinator => None,
                };
                let frames = outputs.get(&edge.producer).cloned().ok_or_else(|| {
                    DistributedError::InvalidPlan(format!(
                        "missing output of producer fragment {}",
                        edge.producer
                    ))
                })?;
                let frames = route_frames(plan, edge, frames)?;
                inputs.push(ProducerInput {
                    fragment_id: edge.producer,
                    tablet,
                    frames,
                });
            }
        }
        let mut current = RootData::Streams(inputs);
        for operator in &fragment.operators {
            checkpoint(parent)?;
            match operator {
                FragmentOperator::RemoteExchangeSource { .. } => {}
                FragmentOperator::FinalAggregate {
                    group_by,
                    aggregates,
                } => {
                    let batches = current.flatten();
                    current = RootData::Batches(vec![final_aggregate_batches(
                        &batches, group_by, aggregates,
                    )?]);
                }
                FragmentOperator::MergeSort { keys, limit } => {
                    current = RootData::Batches(match &current {
                        RootData::Streams(_) => {
                            let streams = per_stream_batches(&current);
                            merge_sorted_streams(&streams, keys, *limit)?
                        }
                        RootData::Batches(batches) => sort_batches_local(batches, keys, *limit)?,
                    });
                }
                FragmentOperator::DistributedTopK { k, score } => {
                    current = RootData::Batches(
                        self.coordinator_top_k(
                            plan,
                            &current,
                            *k,
                            score,
                            parent,
                            &authorization_context,
                        )
                        .await?,
                    );
                }
                FragmentOperator::DistributedLimit { limit } => {
                    let batches = current.flatten();
                    current = RootData::Batches(limit_batches(&batches, *limit));
                }
                other => {
                    return Err(DistributedError::Unsupported(format!(
                    "root fragment operator {other:?} is not coordinator-executable in this wave"
                )))
                }
            }
        }
        Ok(current.flatten())
    }

    /// The coordinator-side deterministic top-k merge with adaptive refill
    /// (spec section 12.10): merges every producer's bounded local top-k
    /// under the exact tie-break, refilling tablets whose unseen-score bound
    /// could still contribute winners through the transport.
    async fn coordinator_top_k(
        &self,
        plan: &DistributedPlan,
        data: &RootData,
        k: usize,
        score: &SortKey,
        control: &ExecutionControl,
        authorization_context: &[u8],
    ) -> DistributedResult<Vec<RecordBatch>> {
        // A pre-combined input (chained after another coordinator operator)
        // is a single synthetic stream that never refills.
        let synthetic;
        let inputs: &[ProducerInput] = match data {
            RootData::Streams(inputs) => inputs,
            RootData::Batches(batches) => {
                synthetic = ProducerInput {
                    fragment_id: u32::MAX,
                    tablet: Some(TabletId::ZERO),
                    frames: batches.iter().cloned().map(BatchFrame::data).collect(),
                };
                std::slice::from_ref(&synthetic)
            }
        };
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut batch_tablet: Vec<TabletId> = Vec::new();
        let mut shards: BTreeMap<TabletId, TabletTopK> = BTreeMap::new();
        let mut fragment_of: HashMap<TabletId, FragmentId> = HashMap::new();
        for input in inputs {
            let tablet = input.tablet.ok_or_else(|| {
                DistributedError::InvalidPlan(
                    "distributed top-k inputs must be tablet fragments".to_owned(),
                )
            })?;
            fragment_of.insert(tablet, input.fragment_id);
            let mut rows = Vec::new();
            let mut bound = None;
            for frame in &input.frames {
                let batch = &frame.batch;
                if batch.num_rows() > 0 {
                    let score_index = batch.schema().index_of(&score.column).map_err(|_| {
                        DistributedError::InvalidPlan(format!(
                            "top-k score column `{}` not in schema",
                            score.column
                        ))
                    })?;
                    let score_array = batch.column(score_index).clone();
                    let row_id_array = row_ids(batch)?;
                    for row in 0..batch.num_rows() {
                        rows.push(TopKCandidate {
                            score: score_key(score_array.as_ref(), row)?,
                            tablet,
                            row_id: RowId(row_id_array.value(row)),
                        });
                    }
                }
                batch_tablet.push(tablet);
                all_batches.push(batch.clone());
                match frame.score_bound {
                    ScoreBound::AtMost(next) => bound = Some(next),
                    ScoreBound::Exhausted => bound = None,
                    // No bound reporting: treated as exhausted (documented
                    // producer contract).
                    ScoreBound::Unknown => {}
                }
            }
            shards.insert(
                tablet,
                TabletTopK {
                    tablet,
                    rows,
                    unseen_bound: bound,
                },
            );
        }
        let winners = loop {
            let ordered: Vec<TabletTopK> = shards.values().cloned().collect();
            let merge = merge_top_k(&ordered, k);
            if merge.refill.is_empty() {
                break merge.winners;
            }
            for tablet in merge.refill {
                let fragment_id = fragment_of[&tablet];
                let already = shards[&tablet].rows.len();
                let refill = self
                    .transport
                    .refill_top_k(
                        plan.query_id,
                        &plan.fragments[fragment_id as usize],
                        already,
                        k,
                        FragmentControl {
                            control: control.child_with_deadline(None),
                            max_spill_bytes: plan.fragments[fragment_id as usize].max_spill_bytes,
                            authorization_context: authorization_context.into(),
                        },
                    )
                    .await?;
                let entry = shards.get_mut(&tablet).expect("shard exists");
                if refill.rows.is_empty() && refill.unseen_bound == entry.unseen_bound {
                    return Err(DistributedError::InvalidPlan(format!(
                        "top-k refill for fragment {fragment_id} made no progress"
                    )));
                }
                batch_tablet.push(tablet);
                all_batches.push(refill.payload);
                entry.rows.extend(refill.rows);
                entry.unseen_bound = refill.unseen_bound;
            }
        };
        if winners.is_empty() {
            return Ok(Vec::new());
        }
        // Locate every winner's payload row.
        let mut locations: HashMap<(TabletId, u64), (usize, usize)> = HashMap::new();
        for (batch_index, batch) in all_batches.iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }
            let row_id_array = row_ids(batch)?;
            let tablet = batch_tablet[batch_index];
            for row in 0..batch.num_rows() {
                locations.insert((tablet, row_id_array.value(row)), (batch_index, row));
            }
        }
        let mut order = Vec::with_capacity(winners.len());
        for winner in &winners {
            let location = locations
                .get(&(winner.tablet, winner.row_id.0))
                .ok_or_else(|| {
                    DistributedError::InvalidPlan(format!(
                        "top-k winner {:?} has no payload row",
                        winner.row_id
                    ))
                })?;
            order.push(*location);
        }
        let merged = emit_interleaved(&all_batches, &order)?;
        // Strip the internal row-id column from the result.
        let schema = merged.first().map(|batch| batch.schema()).ok_or_else(|| {
            DistributedError::InvalidPlan("top-k merge produced no output".to_owned())
        })?;
        let keep: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() != TOPK_ROWID_COLUMN)
            .map(|(index, _)| index)
            .collect();
        if keep.len() == schema.fields().len() {
            return Ok(merged);
        }
        merged
            .iter()
            .map(|batch| Ok(batch.project(&keep)?))
            .collect()
    }
}

/// One stream's concatenated payload batches (for the k-way merge).
fn per_stream_batches(data: &RootData) -> Vec<RecordBatch> {
    match data {
        RootData::Streams(inputs) => inputs
            .iter()
            .filter_map(|input| {
                let batches: Vec<RecordBatch> = input
                    .frames
                    .iter()
                    .map(|frame| frame.batch.clone())
                    .collect();
                concat_all(&batches).ok().flatten()
            })
            .collect(),
        RootData::Batches(batches) => batches.clone(),
    }
}

/// A short worker label for fragment errors.
fn worker_label(fragment: &PlanFragment) -> String {
    match fragment.assignment {
        FragmentAssignment::Tablet(tablet) => format!("tablet {tablet}"),
        FragmentAssignment::Coordinator => "coordinator".to_owned(),
    }
}

/// Validates the structural invariants execution relies on.
fn validate_plan_shape(plan: &DistributedPlan) -> DistributedResult<()> {
    if plan.fragments.is_empty() {
        return Err(DistributedError::InvalidPlan(
            "plan has no fragments".to_owned(),
        ));
    }
    for (index, fragment) in plan.fragments.iter().enumerate() {
        if fragment.fragment_id as usize != index {
            return Err(DistributedError::InvalidPlan(
                "fragment ids must equal their vector index".to_owned(),
            ));
        }
    }
    for edge in &plan.exchanges {
        if edge.producer as usize >= plan.fragments.len()
            || edge.consumer as usize >= plan.fragments.len()
        {
            return Err(DistributedError::InvalidPlan(format!(
                "exchange {} references a missing fragment",
                edge.exchange_id
            )));
        }
    }
    let roots = plan
        .fragments
        .iter()
        .filter(|fragment| {
            !plan
                .exchanges
                .iter()
                .any(|edge| edge.producer == fragment.fragment_id)
        })
        .count();
    if roots != 1 {
        return Err(DistributedError::InvalidPlan(format!(
            "plan must have exactly one root fragment, found {roots}"
        )));
    }
    if let Some(root) = plan.root_fragment_id() {
        for fragment in &plan.fragments {
            if fragment.fragment_id != root
                && fragment.assignment == FragmentAssignment::Coordinator
            {
                return Err(DistributedError::InvalidPlan(
                    "only the root fragment may be coordinator-assigned".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

/// Groups non-root fragments into dependency layers (producers first).
fn fragment_layers(
    plan: &DistributedPlan,
    root: FragmentId,
) -> DistributedResult<Vec<Vec<FragmentId>>> {
    fn depth(
        plan: &DistributedPlan,
        fragment: FragmentId,
        memo: &mut [Option<usize>],
        visiting: &mut [bool],
    ) -> DistributedResult<usize> {
        if let Some(depth) = memo[fragment as usize] {
            return Ok(depth);
        }
        if visiting[fragment as usize] {
            return Err(DistributedError::InvalidPlan(
                "exchange graph has a cycle".to_owned(),
            ));
        }
        visiting[fragment as usize] = true;
        let mut best = 0;
        for edge in plan.exchanges_into(fragment) {
            best = best.max(depth(plan, edge.producer, memo, visiting)? + 1);
        }
        visiting[fragment as usize] = false;
        memo[fragment as usize] = Some(best);
        Ok(best)
    }

    let mut memo = vec![None; plan.fragments.len()];
    let mut visiting = vec![false; plan.fragments.len()];
    let mut layers: Vec<Vec<FragmentId>> = Vec::new();
    for fragment in &plan.fragments {
        if fragment.fragment_id == root {
            continue;
        }
        let depth = depth(plan, fragment.fragment_id, &mut memo, &mut visiting)?;
        if layers.len() <= depth {
            layers.resize(depth + 1, Vec::new());
        }
        layers[depth].push(fragment.fragment_id);
    }
    Ok(layers)
}

/// Builds one consumer's input streams from producer outputs, routing each
/// edge per its exchange kind.
fn build_inputs(
    plan: &DistributedPlan,
    consumer: FragmentId,
    outputs: &HashMap<FragmentId, Vec<BatchFrame>>,
) -> DistributedResult<Vec<FragmentStream>> {
    let mut inputs = Vec::new();
    for edge in plan.exchanges_into(consumer) {
        let frames = outputs.get(&edge.producer).cloned().ok_or_else(|| {
            DistributedError::InvalidPlan(format!(
                "missing output of producer fragment {}",
                edge.producer
            ))
        })?;
        let frames = route_frames(plan, edge, frames)?;
        inputs.push(Box::pin(stream::iter(frames.into_iter().map(Ok))) as FragmentStream);
    }
    Ok(inputs)
}

/// Routes one producer's output frames to one consumer per the edge kind:
/// hash-repartitioned rows are split across the producer's sibling edges.
fn route_frames(
    plan: &DistributedPlan,
    edge: &ExchangeDescriptor,
    frames: Vec<BatchFrame>,
) -> DistributedResult<Vec<BatchFrame>> {
    match &edge.kind {
        ExchangeKind::Merge | ExchangeKind::Broadcast => Ok(frames),
        ExchangeKind::HashRepartition { keys } => {
            let mut siblings: Vec<&ExchangeDescriptor> = plan
                .exchanges
                .iter()
                .filter(|candidate| {
                    candidate.producer == edge.producer
                        && matches!(candidate.kind, ExchangeKind::HashRepartition { .. })
                })
                .collect();
            siblings.sort_by_key(|candidate| candidate.consumer);
            let width = siblings.len();
            let index = siblings
                .iter()
                .position(|candidate| candidate.consumer == edge.consumer)
                .ok_or_else(|| {
                    DistributedError::InvalidPlan(format!(
                        "exchange {} is missing from its repartition boundary",
                        edge.exchange_id
                    ))
                })?;
            repartition_frames(&frames, keys, width, index)
        }
    }
}

/// Bridges a types-level [`QueryId`] onto the query registry's id space.
fn to_registry_query_id(query_id: &QueryId) -> DistributedResult<crate::query_registry::QueryId> {
    query_id
        .to_hex()
        .parse()
        .map_err(|error| DistributedError::InvalidPlan(format!("query id bridge failed: {error}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    /// Deterministic tablet id for tests.
    fn tablet(n: u8) -> TabletId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        TabletId::from_bytes(bytes)
    }

    /// Seeded RNG (SplitMix64) for reproducible property tests.
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

    #[derive(Default)]
    struct StaticLocator {
        tablets: HashMap<String, Vec<TabletId>>,
        specs: HashMap<String, PartitionSpec>,
    }

    impl TabletLocator for StaticLocator {
        fn tablets_for_table(&self, table: &str) -> DistributedResult<Vec<TabletId>> {
            self.tablets
                .get(table)
                .cloned()
                .ok_or_else(|| DistributedError::UnknownTable(table.to_owned()))
        }

        fn partitioning(&self, table: &str) -> DistributedResult<PartitionSpec> {
            self.specs
                .get(table)
                .cloned()
                .ok_or_else(|| DistributedError::UnknownTable(table.to_owned()))
        }
    }

    struct StaticMetadata {
        version: MetadataVersion,
        stats: HashMap<String, TableStats>,
    }

    impl ClusterMetadata for StaticMetadata {
        fn metadata_version(&self) -> MetadataVersion {
            self.version
        }

        fn table_stats(&self, table: &str) -> DistributedResult<TableStats> {
            self.stats
                .get(table)
                .copied()
                .ok_or_else(|| DistributedError::UnknownTable(table.to_owned()))
        }
    }

    /// Builds locator + metadata for a set of tables.
    fn world(
        entries: &[(&str, &[TabletId], PartitionSpec, TableStats)],
    ) -> (StaticLocator, StaticMetadata) {
        let mut locator = StaticLocator::default();
        let mut stats = HashMap::new();
        for (table, tablets, spec, stat) in entries {
            locator
                .tablets
                .insert((*table).to_owned(), tablets.to_vec());
            locator.specs.insert((*table).to_owned(), spec.clone());
            stats.insert((*table).to_owned(), *stat);
        }
        (
            locator,
            StaticMetadata {
                version: MetadataVersion::new(7),
                stats,
            },
        )
    }

    fn scan(table: &str) -> LogicalPlanLite {
        LogicalPlanLite::Scan {
            table: table.to_owned(),
            predicate: None,
            projection: Vec::new(),
        }
    }

    fn desc(root: LogicalPlanLite) -> PlanDescription {
        PlanDescription {
            query_id: QueryId::new_random(),
            root,
            options: PlannerOptions::default(),
        }
    }

    fn hash_spec(column: &str) -> PartitionSpec {
        PartitionSpec::Hash {
            columns: vec![column.to_owned()],
            buckets: 16,
        }
    }

    fn stats(rows: u64, bytes: u64) -> TableStats {
        TableStats {
            row_count: rows,
            total_bytes: bytes,
        }
    }

    fn i64_batch(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
        let schema = Schema::new(
            columns
                .iter()
                .map(|(name, _)| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        );
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values.clone())) as ArrayRef)
            .collect();
        RecordBatch::try_new(schema.into(), arrays).unwrap()
    }

    fn score_batch(scores: Vec<u64>, row_ids: Vec<u64>, payloads: Vec<i64>) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("score", DataType::UInt64, true),
            Field::new(TOPK_ROWID_COLUMN, DataType::UInt64, false),
            Field::new("payload", DataType::Int64, true),
        ]);
        RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(scores)),
                Arc::new(UInt64Array::from(row_ids)),
                Arc::new(Int64Array::from(payloads)),
            ],
        )
        .unwrap()
    }

    fn collect_i64(batches: &[RecordBatch], column: &str) -> Vec<i64> {
        let mut values = Vec::new();
        for batch in batches {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(
                    !array.is_null(row),
                    "column {column} has an unexpected null"
                );
                values.push(array.value(row));
            }
        }
        values
    }

    fn collect_u64(batches: &[RecordBatch], column: &str) -> Vec<u64> {
        let mut values = Vec::new();
        for batch in batches {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                values.push(array.value(row));
            }
        }
        values
    }

    fn collect_f64(batches: &[RecordBatch], column: &str) -> Vec<Option<f64>> {
        let mut values = Vec::new();
        for batch in batches {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                values.push((!array.is_null(row)).then(|| array.value(row)));
            }
        }
        values
    }

    fn operators(plan: &DistributedPlan, fragment_id: FragmentId) -> &[FragmentOperator] {
        &plan.fragments[fragment_id as usize].operators
    }

    // -----------------------------------------------------------------------
    // Plan shape tests
    // -----------------------------------------------------------------------

    #[test]
    fn scan_plan_shape_estimates_and_spill() {
        let tablets = [tablet(1), tablet(2), tablet(3)];
        let (locator, metadata) = world(&[(
            "t",
            &tablets,
            hash_spec("id"),
            stats(3_000, 3 * 1024 * 1024),
        )]);
        let description = desc(scan("t"));
        let plan = distribute(&description, &locator, &metadata).unwrap();
        assert_eq!(plan.query_id, description.query_id);
        assert_eq!(plan.metadata_version, MetadataVersion::new(7));
        assert_eq!(
            plan.fragments.len(),
            4,
            "3 scan fragments + coordinator gather"
        );
        for (index, id) in tablets.iter().enumerate() {
            let fragment = &plan.fragments[index];
            assert_eq!(fragment.assignment, FragmentAssignment::Tablet(*id));
            assert!(
                matches!(
                    operators(&plan, fragment.fragment_id)[0],
                    FragmentOperator::TabletScan { .. }
                ),
                "fragment {index} starts with a tablet scan"
            );
            assert!(
                matches!(
                    operators(&plan, fragment.fragment_id).last().unwrap(),
                    FragmentOperator::RemoteExchangeSink { .. }
                ),
                "fragment {index} ends with a sink"
            );
            assert_eq!(fragment.estimated_rows, 1_000);
            assert_eq!(fragment.estimated_bytes, 1024 * 1024);
            assert_eq!(
                fragment.max_spill_bytes,
                DEFAULT_MAX_SPILL_BYTES_PER_FRAGMENT
            );
        }
        let root = plan.root_fragment_id().unwrap();
        assert_eq!(root, 3);
        assert_eq!(
            plan.fragments[3].assignment,
            FragmentAssignment::Coordinator
        );
        assert_eq!(plan.exchanges.len(), 3);
        for edge in &plan.exchanges {
            assert_eq!(edge.kind, ExchangeKind::Merge);
            assert_eq!(edge.consumer, root);
            assert_eq!(
                edge.schema_fingerprint,
                plan.exchanges[0].schema_fingerprint
            );
        }
        // The configured spill allowance is stamped on every fragment.
        let mut custom = desc(scan("t"));
        custom.options.max_spill_bytes_per_fragment = 1_234;
        let plan = distribute(&custom, &locator, &metadata).unwrap();
        assert!(plan
            .fragments
            .iter()
            .all(|fragment| fragment.max_spill_bytes == 1_234));
    }

    #[test]
    fn fragment_control_begin_spill_opens_core_spill_session() {
        use mongreldb_core::ExecutionControl;
        use mongreldb_types::ids::QueryId;

        let dir = tempfile::tempdir().unwrap();
        // Real shipped path: Database::create owns the SpillManager.
        let db = mongreldb_core::Database::create(dir.path()).unwrap();
        let control = FragmentControl {
            control: ExecutionControl::new(None),
            max_spill_bytes: 64 * 1024,
            authorization_context: Arc::from([]),
        };
        let session = control
            .begin_spill(db.spill_manager(), QueryId::from_bytes([0xAB; 16]))
            .expect("begin_spill binds planner allowance to core SpillManager");
        let stats = db.spill_manager().stats();
        assert_eq!(
            stats.global_budget_bytes,
            db.spill_manager().config().global_bytes
        );
        drop(session);
    }

    #[test]
    fn aggregate_plan_shape_grouped_and_ungrouped() {
        let tablets = [tablet(1), tablet(2), tablet(3)];
        let (locator, metadata) = world(&[("t", &tablets, hash_spec("id"), stats(900, 900_000))]);
        let aggregates = vec![
            AggregateExpr {
                function: AggregateFunction::Count,
                column: None,
            },
            AggregateExpr {
                function: AggregateFunction::Sum,
                column: Some("v".to_owned()),
            },
        ];
        // Grouped: partial on producers, hash-repartition exchange, final at
        // the coordinator.
        let grouped = LogicalPlanLite::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec!["g".to_owned()],
            aggregates: aggregates.clone(),
        };
        let plan = distribute(&desc(grouped), &locator, &metadata).unwrap();
        assert_eq!(plan.fragments.len(), 4);
        for index in 0..3 {
            assert!(matches!(
                operators(&plan, index as FragmentId)[1],
                FragmentOperator::PartialAggregate { .. }
            ));
        }
        assert_eq!(plan.exchanges.len(), 3);
        for edge in &plan.exchanges {
            assert_eq!(
                edge.kind,
                ExchangeKind::HashRepartition {
                    keys: vec!["g".to_owned()]
                }
            );
        }
        let root = plan.root_fragment_id().unwrap();
        assert!(matches!(
            operators(&plan, root).last().unwrap(),
            FragmentOperator::FinalAggregate { .. }
        ));
        // Ungrouped: merge exchange, single-row estimate.
        let ungrouped = LogicalPlanLite::Aggregate {
            input: Box::new(scan("t")),
            group_by: Vec::new(),
            aggregates: aggregates[..1].to_vec(),
        };
        let plan = distribute(&desc(ungrouped), &locator, &metadata).unwrap();
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| edge.kind == ExchangeKind::Merge));
        let root = plan.root_fragment_id().unwrap();
        assert_eq!(plan.fragments[root as usize].estimated_rows, 1);
    }

    #[test]
    fn colocated_join_plan_fuses_scans_without_exchange() {
        let tablets = [tablet(1), tablet(2), tablet(3)];
        let (locator, metadata) = world(&[
            (
                "a",
                &tablets,
                hash_spec("id"),
                stats(6_000, 6 * 1024 * 1024),
            ),
            (
                "b",
                &tablets,
                hash_spec("id"),
                stats(3_000, 3 * 1024 * 1024),
            ),
        ]);
        let join = LogicalPlanLite::Join {
            left: Box::new(scan("a")),
            right: Box::new(scan("b")),
            on: vec![JoinKey {
                left: "id".to_owned(),
                right: "id".to_owned(),
            }],
        };
        let plan = distribute(&desc(join), &locator, &metadata).unwrap();
        assert_eq!(plan.fragments.len(), 4, "3 fused fragments + gather");
        for index in 0..3 {
            let ops = operators(&plan, index as FragmentId);
            assert!(
                matches!(&ops[0], FragmentOperator::TabletScan { table, .. } if table == "a"),
                "left scan first: {ops:?}"
            );
            assert!(
                matches!(&ops[1], FragmentOperator::TabletScan { table, .. } if table == "b"),
                "right scan second: {ops:?}"
            );
            assert!(
                matches!(&ops[2], FragmentOperator::DistributedHashJoin { .. }),
                "fused hash join: {ops:?}"
            );
        }
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| edge.kind == ExchangeKind::Merge),
            "a colocated join has no shuffle: {:?}",
            plan.exchanges
        );
        assert_eq!(plan.exchanges.len(), 3, "only the gather edges remain");
    }

    #[test]
    fn broadcast_join_plan_replicates_the_small_side() {
        let big_tablets = [tablet(1), tablet(2), tablet(3)];
        let small_tablets = [tablet(4), tablet(5)];
        let (locator, metadata) = world(&[
            (
                "big",
                &big_tablets,
                hash_spec("id"),
                stats(8_000, 64 * 1024 * 1024),
            ),
            (
                "small",
                &small_tablets,
                hash_spec("key"),
                stats(100, 1024 * 1024),
            ),
        ]);
        let join = LogicalPlanLite::Join {
            left: Box::new(scan("big")),
            right: Box::new(scan("small")),
            on: vec![JoinKey {
                left: "id".to_owned(),
                right: "key".to_owned(),
            }],
        };
        let plan = distribute(&desc(join), &locator, &metadata).unwrap();
        // 3 big + 2 small + gather.
        assert_eq!(plan.fragments.len(), 6);
        let root = plan.root_fragment_id().unwrap();
        let broadcast_edges: Vec<&ExchangeDescriptor> = plan
            .exchanges
            .iter()
            .filter(|edge| edge.kind == ExchangeKind::Broadcast)
            .collect();
        assert_eq!(
            broadcast_edges.len(),
            6,
            "each small producer x each big fragment"
        );
        for edge in &broadcast_edges {
            assert!(edge.producer >= 3, "small side produces the broadcast");
            assert!(edge.consumer < 3, "big side consumes the broadcast");
        }
        for index in 0..3 {
            let ops = operators(&plan, index as FragmentId);
            let last_transform = ops
                .iter()
                .rfind(|op| !matches!(op, FragmentOperator::RemoteExchangeSink { .. }))
                .unwrap();
            assert!(matches!(
                last_transform,
                FragmentOperator::BroadcastJoin {
                    build_side: BuildSide::Right,
                    ..
                }
            ));
            let sources = ops
                .iter()
                .filter(|op| matches!(op, FragmentOperator::RemoteExchangeSource { .. }))
                .count();
            assert_eq!(sources, 2, "one source per small producer");
        }
        assert!(plan
            .exchanges
            .iter()
            .filter(|edge| edge.consumer == root)
            .all(|edge| edge.kind == ExchangeKind::Merge && edge.producer < 3));
    }

    #[test]
    fn repartition_join_plan_shuffles_both_sides() {
        let left_tablets = [tablet(1), tablet(2)];
        let right_tablets = [tablet(3), tablet(4), tablet(5)];
        let (locator, metadata) = world(&[
            (
                "l",
                &left_tablets,
                hash_spec("a"),
                stats(8_000, 64 * 1024 * 1024),
            ),
            (
                "r",
                &right_tablets,
                hash_spec("b"),
                stats(9_000, 96 * 1024 * 1024),
            ),
        ]);
        let join = LogicalPlanLite::Join {
            left: Box::new(scan("l")),
            right: Box::new(scan("r")),
            on: vec![JoinKey {
                left: "a".to_owned(),
                right: "b".to_owned(),
            }],
        };
        let plan = distribute(&desc(join), &locator, &metadata).unwrap();
        // 2 left scans + 3 right scans + 3 join fragments + gather.
        assert_eq!(plan.fragments.len(), 9);
        let root = plan.root_fragment_id().unwrap();
        let shuffles: Vec<&ExchangeDescriptor> = plan
            .exchanges
            .iter()
            .filter(|edge| matches!(edge.kind, ExchangeKind::HashRepartition { .. }))
            .collect();
        assert_eq!(shuffles.len(), 2 * 3 + 3 * 3);
        let left_keys: Vec<&ExchangeDescriptor> = shuffles
            .iter()
            .filter(|edge| edge.producer < 2)
            .copied()
            .collect();
        assert!(
            left_keys
                .iter()
                .all(|edge| matches!(&edge.kind, ExchangeKind::HashRepartition { keys } if keys == &vec!["a".to_owned()]))
        );
        for join_fragment in 5..8 {
            let fragment = &plan.fragments[join_fragment];
            assert_eq!(
                fragment.assignment,
                FragmentAssignment::Tablet(right_tablets[(join_fragment - 5) % 3]),
                "join fragments round-robin over the larger side's tablets"
            );
            let ops = operators(&plan, fragment.fragment_id);
            let sources = ops
                .iter()
                .filter(|op| matches!(op, FragmentOperator::RemoteExchangeSource { .. }))
                .count();
            assert_eq!(sources, 5, "2 left + 3 right sources: {ops:?}");
            let last_transform = ops
                .iter()
                .rfind(|op| !matches!(op, FragmentOperator::RemoteExchangeSink { .. }))
                .unwrap();
            assert!(matches!(
                last_transform,
                FragmentOperator::RepartitionJoin { .. }
            ));
        }
        assert!(plan
            .exchanges
            .iter()
            .filter(|edge| edge.consumer == root)
            .all(|edge| edge.kind == ExchangeKind::Merge));
    }

    #[test]
    fn sort_and_limit_plan_shapes() {
        let tablets = [tablet(1), tablet(2), tablet(3)];
        let (locator, metadata) = world(&[("t", &tablets, hash_spec("id"), stats(900, 900_000))]);

        // Single descending key + limit: distributed top-k on both sides.
        let topk = LogicalPlanLite::Sort {
            input: Box::new(scan("t")),
            keys: vec![SortKey {
                column: "score".to_owned(),
                descending: true,
            }],
            limit: Some(10),
        };
        let plan = distribute(&desc(topk), &locator, &metadata).unwrap();
        for index in 0..3 {
            assert!(matches!(
                operators(&plan, index as FragmentId)[1],
                FragmentOperator::DistributedTopK { k: 10, .. }
            ));
        }
        let root = plan.root_fragment_id().unwrap();
        assert!(matches!(
            operators(&plan, root).last().unwrap(),
            FragmentOperator::DistributedTopK { k: 10, .. }
        ));
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| edge.kind == ExchangeKind::Merge));

        // Plain sort: merge sort on both sides.
        let sort = LogicalPlanLite::Sort {
            input: Box::new(scan("t")),
            keys: vec![SortKey {
                column: "x".to_owned(),
                descending: false,
            }],
            limit: None,
        };
        let plan = distribute(&desc(sort), &locator, &metadata).unwrap();
        for index in 0..3 {
            assert!(matches!(
                operators(&plan, index as FragmentId)[1],
                FragmentOperator::MergeSort { limit: None, .. }
            ));
        }
        let root = plan.root_fragment_id().unwrap();
        assert!(matches!(
            operators(&plan, root).last().unwrap(),
            FragmentOperator::MergeSort { limit: None, .. }
        ));

        // Multi-key sort with a limit is NOT a top-k (single descending key
        // only): merge sort with the limit pushed down.
        let multi = LogicalPlanLite::Sort {
            input: Box::new(scan("t")),
            keys: vec![
                SortKey {
                    column: "a".to_owned(),
                    descending: false,
                },
                SortKey {
                    column: "b".to_owned(),
                    descending: true,
                },
            ],
            limit: Some(5),
        };
        let plan = distribute(&desc(multi), &locator, &metadata).unwrap();
        for index in 0..3 {
            assert!(matches!(
                operators(&plan, index as FragmentId)[1],
                FragmentOperator::MergeSort { limit: Some(5), .. }
            ));
        }
        let root = plan.root_fragment_id().unwrap();
        assert!(matches!(
            operators(&plan, root).last().unwrap(),
            FragmentOperator::MergeSort { limit: Some(5), .. }
        ));

        // Bare limit: pushed down and re-applied at the coordinator.
        let limit = LogicalPlanLite::Limit {
            input: Box::new(scan("t")),
            limit: 7,
        };
        let plan = distribute(&desc(limit), &locator, &metadata).unwrap();
        for index in 0..3 {
            assert!(matches!(
                operators(&plan, index as FragmentId)[1],
                FragmentOperator::DistributedLimit { limit: 7 }
            ));
        }
        let root = plan.root_fragment_id().unwrap();
        assert!(matches!(
            operators(&plan, root).last().unwrap(),
            FragmentOperator::DistributedLimit { limit: 7 }
        ));

        // A limit over a scalar aggregate stays in the one coordinator
        // fragment (no extra hop).
        let chained = LogicalPlanLite::Limit {
            input: Box::new(LogicalPlanLite::Aggregate {
                input: Box::new(scan("t")),
                group_by: Vec::new(),
                aggregates: vec![AggregateExpr {
                    function: AggregateFunction::Count,
                    column: None,
                }],
            }),
            limit: 5,
        };
        let plan = distribute(&desc(chained), &locator, &metadata).unwrap();
        assert_eq!(plan.fragments.len(), 4);
        let root = plan.root_fragment_id().unwrap();
        let ops = operators(&plan, root);
        assert!(matches!(
            ops[ops.len() - 2],
            FragmentOperator::FinalAggregate { .. }
        ));
        assert!(matches!(
            ops[ops.len() - 1],
            FragmentOperator::DistributedLimit { limit: 5 }
        ));
    }

    #[test]
    fn planner_rejects_invalid_inputs() {
        let tablets = [tablet(1)];
        let (locator, metadata) = world(&[("t", &tablets, hash_spec("id"), stats(10, 100))]);
        // Unknown table.
        let error = distribute(&desc(scan("nope")), &locator, &metadata).unwrap_err();
        assert!(
            matches!(error, DistributedError::UnknownTable(_)),
            "{error}"
        );
        // Empty layout.
        let (locator, metadata) = world(&[("e", &[], hash_spec("id"), stats(0, 0))]);
        let error = distribute(&desc(scan("e")), &locator, &metadata).unwrap_err();
        assert!(
            matches!(error, DistributedError::EmptyLayout { .. }),
            "{error}"
        );
        let (locator, metadata) = world(&[("t", &tablets, hash_spec("id"), stats(10, 100))]);
        // Join without keys.
        let join = LogicalPlanLite::Join {
            left: Box::new(scan("t")),
            right: Box::new(scan("t")),
            on: Vec::new(),
        };
        let error = distribute(&desc(join), &locator, &metadata).unwrap_err();
        assert!(matches!(error, DistributedError::InvalidPlan(_)), "{error}");
        // Sum without a column.
        let aggregate = LogicalPlanLite::Aggregate {
            input: Box::new(scan("t")),
            group_by: Vec::new(),
            aggregates: vec![AggregateExpr {
                function: AggregateFunction::Sum,
                column: None,
            }],
        };
        let error = distribute(&desc(aggregate), &locator, &metadata).unwrap_err();
        assert!(matches!(error, DistributedError::InvalidPlan(_)), "{error}");
        // Sort without keys.
        let sort = LogicalPlanLite::Sort {
            input: Box::new(scan("t")),
            keys: Vec::new(),
            limit: None,
        };
        let error = distribute(&desc(sort), &locator, &metadata).unwrap_err();
        assert!(matches!(error, DistributedError::InvalidPlan(_)), "{error}");
    }

    // -----------------------------------------------------------------------
    // Distributed top-k: pure merge + adaptive refill
    // -----------------------------------------------------------------------

    fn candidate(score: u64, tablet_n: u8, row_id: u64) -> TopKCandidate {
        TopKCandidate {
            score,
            tablet: tablet(tablet_n),
            row_id: RowId(row_id),
        }
    }

    #[test]
    fn topk_tie_break_is_exact() {
        // Final score descending, tablet id ascending, RowId ascending.
        let mut rows = vec![
            candidate(5, 2, 0),
            candidate(5, 1, 2),
            candidate(7, 3, 0),
            candidate(5, 1, 0),
            candidate(3, 1, 0),
        ];
        rows.sort_by(topk_cmp);
        assert_eq!(
            rows,
            vec![
                candidate(7, 3, 0),
                candidate(5, 1, 0),
                candidate(5, 1, 2),
                candidate(5, 2, 0),
                candidate(3, 1, 0),
            ]
        );
    }

    #[test]
    fn merge_top_k_needs_no_refill_when_tablets_are_exhausted() {
        let shards = vec![
            TabletTopK {
                tablet: tablet(1),
                rows: vec![candidate(100, 1, 0), candidate(90, 1, 1)],
                unseen_bound: None,
            },
            TabletTopK {
                tablet: tablet(2),
                rows: vec![candidate(96, 2, 0)],
                unseen_bound: None,
            },
        ];
        let merge = merge_top_k(&shards, 2);
        assert_eq!(
            merge.winners,
            vec![candidate(100, 1, 0), candidate(96, 2, 0)]
        );
        assert!(merge.refill.is_empty());
    }

    #[test]
    fn merge_top_k_refill_rules_follow_the_bounds() {
        // Bound below the k-th winner: no refill possible.
        let shards = vec![
            TabletTopK {
                tablet: tablet(1),
                rows: vec![candidate(100, 1, 0), candidate(90, 1, 1)],
                unseen_bound: Some(95),
            },
            TabletTopK {
                tablet: tablet(2),
                rows: vec![candidate(96, 2, 0)],
                unseen_bound: None,
            },
        ];
        let merge = merge_top_k(&shards, 2);
        assert!(
            merge.refill.is_empty(),
            "unseen scores of tablet 1 are at most 95 < 96: {:?}",
            merge.refill
        );
        // Bound above the k-th winner: refill required.
        let shards = vec![
            TabletTopK {
                tablet: tablet(1),
                rows: vec![candidate(100, 1, 0), candidate(90, 1, 1)],
                unseen_bound: Some(97),
            },
            TabletTopK {
                tablet: tablet(2),
                rows: vec![candidate(96, 2, 0)],
                unseen_bound: None,
            },
        ];
        let merge = merge_top_k(&shards, 2);
        assert_eq!(merge.refill, vec![tablet(1)]);
        // Tie at the k-th winner's score with a better tablet id: the
        // conservative tie case refills.
        let shards = vec![
            TabletTopK {
                tablet: tablet(1),
                rows: vec![candidate(100, 1, 0)],
                unseen_bound: Some(96),
            },
            TabletTopK {
                tablet: tablet(2),
                rows: vec![candidate(96, 2, 9)],
                unseen_bound: None,
            },
        ];
        let merge = merge_top_k(&shards, 2);
        assert_eq!(
            merge.refill,
            vec![tablet(1)],
            "(96, tablet 1, min row id) ranks better than (96, tablet 2, 9)"
        );
        // Fewer than k candidates with unseen rows: refill.
        let shards = vec![
            TabletTopK {
                tablet: tablet(1),
                rows: vec![candidate(5, 1, 0)],
                unseen_bound: Some(1),
            },
            TabletTopK {
                tablet: tablet(2),
                rows: Vec::new(),
                unseen_bound: None,
            },
        ];
        let merge = merge_top_k(&shards, 3);
        assert_eq!(merge.winners, vec![candidate(5, 1, 0)]);
        assert_eq!(merge.refill, vec![tablet(1)]);
        // k = 0 is trivially exact.
        let merge = merge_top_k(&shards, 0);
        assert!(merge.winners.is_empty() && merge.refill.is_empty());
    }

    #[test]
    fn exact_top_k_fills_winners_via_adaptive_refill() {
        // Tablet 1 holds 3 of the true top-3 but only emits 2 up front.
        let tablet_one_rows = vec![
            candidate(100, 1, 0),
            candidate(99, 1, 1),
            candidate(98, 1, 2),
            candidate(97, 1, 3),
        ];
        let shards = vec![
            TabletTopK {
                tablet: tablet(1),
                rows: tablet_one_rows[..2].to_vec(),
                unseen_bound: Some(98),
            },
            TabletTopK {
                tablet: tablet(2),
                rows: vec![candidate(96, 2, 0), candidate(95, 2, 1)],
                unseen_bound: None,
            },
        ];
        let remaining = tablet_one_rows.clone();
        let result = exact_top_k(3, shards, move |tablet_id| {
            assert_eq!(tablet_id, tablet(1));
            TabletTopK {
                tablet: tablet_id,
                rows: remaining[2..].to_vec(),
                unseen_bound: None,
            }
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                candidate(100, 1, 0),
                candidate(99, 1, 1),
                candidate(98, 1, 2),
            ]
        );
    }

    #[test]
    fn exact_top_k_rejects_a_stuck_refill() {
        let shards = vec![TabletTopK {
            tablet: tablet(1),
            rows: Vec::new(),
            unseen_bound: Some(10),
        }];
        let error = exact_top_k(1, shards, |tablet| TabletTopK {
            tablet,
            rows: Vec::new(),
            unseen_bound: Some(10),
        })
        .unwrap_err();
        assert!(matches!(error, DistributedError::InvalidPlan(_)), "{error}");
    }

    #[test]
    fn exact_top_k_matches_single_node_oracle_across_1000_sharded_datasets() {
        let mut total_refills = 0usize;
        for seed in 0..1_000u64 {
            let mut rng = SplitMix64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));
            let tablet_count = 1 + rng.below(8) as usize;
            let k = rng.below(30) as usize;
            let batch = 1 + rng.below(4) as usize;
            let mut oracle_rows = Vec::new();
            let mut per_tablet: HashMap<TabletId, Vec<TopKCandidate>> = HashMap::new();
            for index in 0..tablet_count {
                let tablet = tablet(index as u8 + 1);
                let row_count = rng.below(40) as usize;
                let mut rows: Vec<TopKCandidate> = (0..row_count)
                    .map(|row| TopKCandidate {
                        score: rng.below(12),
                        tablet,
                        row_id: RowId(row as u64),
                    })
                    .collect();
                rows.sort_by(topk_cmp);
                oracle_rows.extend(rows.iter().copied());
                per_tablet.insert(tablet, rows);
            }
            let mut oracle = oracle_rows;
            oracle.sort_by(topk_cmp);
            oracle.truncate(k);
            let contribution = |tablet: TabletId, offset: usize| -> TabletTopK {
                let rows = &per_tablet[&tablet];
                let start = offset.min(rows.len());
                let end = (offset + batch).min(rows.len());
                TabletTopK {
                    tablet,
                    rows: rows[start..end].to_vec(),
                    unseen_bound: rows.get(end).map(|candidate| candidate.score),
                }
            };
            let initial: Vec<TabletTopK> = (0..tablet_count)
                .map(|index| contribution(tablet(index as u8 + 1), 0))
                .collect();
            let mut returned: HashMap<TabletId, usize> = (0..tablet_count)
                .map(|index| (tablet(index as u8 + 1), batch))
                .collect();
            let result = exact_top_k(k, initial, |tablet| {
                total_refills += 1;
                let offset = returned[&tablet];
                returned.insert(tablet, offset + batch);
                contribution(tablet, offset)
            })
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
            assert_eq!(result, oracle, "seed {seed}: k={k} batch={batch}");
        }
        assert!(
            total_refills > 0,
            "the property run must actually exercise adaptive refill"
        );
    }

    // -----------------------------------------------------------------------
    // Execution skeleton: merge operators, cancellation, leases
    // -----------------------------------------------------------------------

    /// Builds a coordinator + in-memory transport + registry over one
    /// executor.
    fn coordinator_with(
        executor: Arc<dyn FragmentExecutor>,
    ) -> (
        Arc<Coordinator>,
        Arc<InMemoryTransport>,
        Arc<SqlQueryRegistry>,
    ) {
        let transport = Arc::new(InMemoryTransport::new(executor));
        let registry = Arc::new(SqlQueryRegistry::default());
        let coordinator = Arc::new(Coordinator::new(
            Arc::clone(&transport) as Arc<dyn FragmentTransport>,
            Arc::clone(&registry),
        ));
        (coordinator, transport, registry)
    }

    /// An executor that signals arrival and then waits for cancellation —
    /// the fixture for cancellation/lease tests.
    struct BlockingExecutor {
        barrier: Arc<tokio::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl FragmentExecutor for BlockingExecutor {
        async fn execute(
            &self,
            _fragment: &PlanFragment,
            _inputs: Vec<FragmentStream>,
            control: FragmentControl,
        ) -> DistributedResult<FragmentStream> {
            self.barrier.wait().await;
            control.control.cancelled().await;
            Err(DistributedError::Cancelled(control.control.reason()))
        }
    }

    /// A scan-only plan over `table` with 3 tablets, suitable for
    /// cancellation fixtures.
    fn scan_plan(table: &str) -> (DistributedPlan, Vec<TabletId>) {
        let tablets = [tablet(1), tablet(2), tablet(3)];
        let (locator, metadata) = world(&[(
            table,
            &tablets,
            hash_spec("id"),
            stats(3_000, 3 * 1024 * 1024),
        )]);
        let plan = distribute(&desc(scan(table)), &locator, &metadata).unwrap();
        (plan, tablets.to_vec())
    }

    #[tokio::test]
    async fn scan_gather_returns_all_rows() {
        let store = Arc::new(InMemoryTableStore::new());
        let tablets = [tablet(1), tablet(2), tablet(3)];
        store.insert("t", tablets[0], i64_batch(&[("v", vec![1, 2])]));
        store.insert("t", tablets[1], i64_batch(&[("v", vec![3])]));
        // Tablet 3 intentionally holds no rows.
        let (plan, _) = scan_plan("t");
        let (coordinator, _, _) = coordinator_with(Arc::new(InMemoryFragmentExecutor::new(store)));
        let batches = coordinator.execute(&plan).await.unwrap();
        let mut values = collect_i64(&batches, "v");
        values.sort_unstable();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn remote_arrow_ipc_transport_gathers_with_pull_backpressure() {
        let store = Arc::new(InMemoryTableStore::new());
        let tablets = [tablet(1), tablet(2), tablet(3)];
        store.insert("t", tablets[0], i64_batch(&[("v", vec![1, 2])]));
        store.insert("t", tablets[1], i64_batch(&[("v", vec![3])]));
        let executor: Arc<dyn FragmentExecutor> = Arc::new(InMemoryFragmentExecutor::new(store));
        let endpoint = Arc::new(RemoteFragmentEndpoint::new(executor));
        let client: Arc<dyn FragmentRpcClient> =
            Arc::new(LoopbackFragmentRpcClient::new(Arc::clone(&endpoint)));
        let transport: Arc<dyn FragmentTransport> = Arc::new(RemoteFragmentTransport::new(client));
        let coordinator = Coordinator::new(transport, Arc::new(SqlQueryRegistry::default()));
        let (plan, _) = scan_plan("t");

        let batches = coordinator.execute(&plan).await.unwrap();
        let mut values = collect_i64(&batches, "v");
        values.sort_unstable();
        assert_eq!(values, vec![1, 2, 3]);
        assert_eq!(
            endpoint.active_executions(),
            0,
            "terminal pulls release every worker cursor"
        );
        // P0.4-T6: fragment lifecycle metrics observe starts/pulls/completes and bytes.
        let metrics = endpoint.lifecycle_metrics();
        assert!(metrics.starts >= 1, "starts={}", metrics.starts);
        assert!(metrics.pulls >= 1, "pulls={}", metrics.pulls);
        assert!(metrics.completes >= 1, "completes={}", metrics.completes);
        assert!(metrics.bytes_in > 0, "bytes_in={}", metrics.bytes_in);
        assert!(metrics.bytes_out > 0, "bytes_out={}", metrics.bytes_out);
        assert_eq!(metrics.active_executions, 0);
    }

    #[tokio::test]
    async fn merge_sort_matches_single_node_sort() {
        let store = Arc::new(InMemoryTableStore::new());
        let tablets = [tablet(1), tablet(2), tablet(3)];
        store.insert("s", tablets[0], i64_batch(&[("x", vec![5, 1, 9])]));
        store.insert("s", tablets[1], i64_batch(&[("x", vec![3, 7])]));
        store.insert("s", tablets[2], i64_batch(&[("x", vec![8, 2, 6, 4])]));
        let (locator, metadata) = world(&[("s", &tablets, hash_spec("id"), stats(9, 900))]);
        let (coordinator, _, _) = coordinator_with(Arc::new(InMemoryFragmentExecutor::new(store)));

        // Full merge sort.
        let sort = LogicalPlanLite::Sort {
            input: Box::new(scan("s")),
            keys: vec![SortKey {
                column: "x".to_owned(),
                descending: false,
            }],
            limit: None,
        };
        let plan = distribute(&desc(sort), &locator, &metadata).unwrap();
        let batches = coordinator.execute(&plan).await.unwrap();
        assert_eq!(collect_i64(&batches, "x"), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // Bounded merge sort.
        let sort = LogicalPlanLite::Sort {
            input: Box::new(scan("s")),
            keys: vec![SortKey {
                column: "x".to_owned(),
                descending: false,
            }],
            limit: Some(4),
        };
        let plan = distribute(&desc(sort), &locator, &metadata).unwrap();
        let batches = coordinator.execute(&plan).await.unwrap();
        assert_eq!(collect_i64(&batches, "x"), vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn final_aggregate_matches_single_node_aggregate() {
        let store = Arc::new(InMemoryTableStore::new());
        let tablets = [tablet(1), tablet(2), tablet(3)];
        store.insert(
            "t",
            tablets[0],
            i64_batch(&[("g", vec![1, 2, 1, 3]), ("v", vec![10, 20, 30, 40])]),
        );
        store.insert(
            "t",
            tablets[1],
            i64_batch(&[("g", vec![2, 1, 3]), ("v", vec![50, 60, 70])]),
        );
        store.insert(
            "t",
            tablets[2],
            i64_batch(&[("g", vec![3, 2]), ("v", vec![80, 90])]),
        );
        let (locator, metadata) = world(&[("t", &tablets, hash_spec("id"), stats(9, 900))]);
        let (coordinator, _, _) = coordinator_with(Arc::new(InMemoryFragmentExecutor::new(store)));

        let aggregates = vec![
            AggregateExpr {
                function: AggregateFunction::Count,
                column: None,
            },
            AggregateExpr {
                function: AggregateFunction::Sum,
                column: Some("v".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Min,
                column: Some("v".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Max,
                column: Some("v".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Avg,
                column: Some("v".to_owned()),
            },
        ];
        let grouped = LogicalPlanLite::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec!["g".to_owned()],
            aggregates,
        };
        let plan = distribute(&desc(grouped), &locator, &metadata).unwrap();
        let batches = coordinator.execute(&plan).await.unwrap();
        let groups = collect_i64(&batches, "g");
        let counts = collect_i64(&batches, "count_star");
        let sums = collect_i64(&batches, "sum_v");
        let mins = collect_i64(&batches, "min_v");
        let maxs = collect_i64(&batches, "max_v");
        let avgs = collect_f64(&batches, "avg_v");
        // Single-node oracle, group order-independent.
        let mut oracle: HashMap<i64, (i64, i64, i64, i64)> = HashMap::new();
        for (g, v) in [
            (1, 10),
            (2, 20),
            (1, 30),
            (3, 40),
            (2, 50),
            (1, 60),
            (3, 70),
            (3, 80),
            (2, 90),
        ] {
            let entry = oracle.entry(g).or_insert((0, 0, i64::MAX, i64::MIN));
            entry.0 += 1;
            entry.1 += v;
            entry.2 = entry.2.min(v);
            entry.3 = entry.3.max(v);
        }
        assert_eq!(groups.len(), 3);
        for index in 0..groups.len() {
            let (count, sum, min, max) = oracle[&groups[index]];
            assert_eq!(counts[index], count, "count for g={}", groups[index]);
            assert_eq!(sums[index], sum, "sum for g={}", groups[index]);
            assert_eq!(mins[index], min, "min for g={}", groups[index]);
            assert_eq!(maxs[index], max, "max for g={}", groups[index]);
            let expected_avg = sum as f64 / count as f64;
            let actual_avg = avgs[index].expect("avg is set");
            assert!(
                (actual_avg - expected_avg).abs() < 1e-12,
                "avg for g={}: {actual_avg} vs {expected_avg}",
                groups[index]
            );
        }

        // Scalar aggregate: one row over all tablets.
        let scalar = LogicalPlanLite::Aggregate {
            input: Box::new(scan("t")),
            group_by: Vec::new(),
            aggregates: vec![
                AggregateExpr {
                    function: AggregateFunction::Count,
                    column: None,
                },
                AggregateExpr {
                    function: AggregateFunction::Sum,
                    column: Some("v".to_owned()),
                },
            ],
        };
        let plan = distribute(&desc(scalar), &locator, &metadata).unwrap();
        let batches = coordinator.execute(&plan).await.unwrap();
        assert_eq!(collect_i64(&batches, "count_star"), vec![9]);
        assert_eq!(collect_i64(&batches, "sum_v"), vec![450]);
    }

    #[tokio::test]
    async fn distributed_top_k_matches_oracle_with_and_without_refill() {
        // Heavy score ties across three tablets; payloads mirror row ids so
        // the exact tie order is observable after `__rowid` is stripped.
        let store = Arc::new(InMemoryTableStore::new());
        let tablets = [tablet(1), tablet(2), tablet(3)];
        store.insert(
            "k",
            tablets[0],
            score_batch(vec![100, 90, 80, 70], vec![0, 1, 2, 3], vec![0, 1, 2, 3]),
        );
        store.insert(
            "k",
            tablets[1],
            score_batch(vec![90, 80, 60], vec![0, 1, 2], vec![0, 1, 2]),
        );
        store.insert(
            "k",
            tablets[2],
            score_batch(vec![90, 80, 50], vec![0, 1, 2], vec![0, 1, 2]),
        );
        let (locator, metadata) = world(&[("k", &tablets, hash_spec("id"), stats(10, 1_000))]);
        let topk = LogicalPlanLite::Sort {
            input: Box::new(scan("k")),
            keys: vec![SortKey {
                column: "score".to_owned(),
                descending: true,
            }],
            limit: Some(5),
        };
        // Single-node oracle under the exact tie-break.
        let mut oracle: Vec<TopKCandidate> = Vec::new();
        for (tablet_n, rows) in [
            (1u8, vec![(100u64, 0u64), (90, 1), (80, 2), (70, 3)]),
            (2u8, vec![(90, 0), (80, 1), (60, 2)]),
            (3u8, vec![(90, 0), (80, 1), (50, 2)]),
        ] {
            for (score, row_id) in rows {
                oracle.push(candidate(score, tablet_n, row_id));
            }
        }
        oracle.sort_by(topk_cmp);
        oracle.truncate(5);
        let expected: Vec<(u64, i64)> = oracle
            .iter()
            .map(|winner| (winner.score, winner.row_id.0 as i64))
            .collect();

        let run = async |emit_batch: Option<usize>| {
            let executor = match emit_batch {
                Some(batch) => InMemoryFragmentExecutor::with_topk_emit_batch(store.clone(), batch),
                None => InMemoryFragmentExecutor::new(store.clone()),
            };
            let (coordinator, transport, _) = coordinator_with(Arc::new(executor));
            let plan = distribute(&desc(topk.clone()), &locator, &metadata).unwrap();
            let batches = coordinator.execute(&plan).await.unwrap();
            (batches, transport)
        };

        // Default emission (up to k rows): exact without refills.
        let (batches, transport) = run(None).await;
        let scores = collect_u64(&batches, "score");
        let payloads = collect_i64(&batches, "payload");
        let actual: Vec<(u64, i64)> = scores.into_iter().zip(payloads).collect();
        assert_eq!(actual, expected);
        assert!(
            transport.refill_log().is_empty(),
            "exact local top-k never needs refill"
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.schema().index_of(TOPK_ROWID_COLUMN).is_err()),
            "the internal row-id column is stripped"
        );

        // Bounded emission (2 rows per round): the coordinator must refill
        // and still match the oracle exactly.
        let (batches, transport) = run(Some(2)).await;
        let scores = collect_u64(&batches, "score");
        let payloads = collect_i64(&batches, "payload");
        let actual: Vec<(u64, i64)> = scores.into_iter().zip(payloads).collect();
        assert_eq!(actual, expected);
        assert!(
            !transport.refill_log().is_empty(),
            "bounded emission must trigger adaptive refill"
        );

        // The same bounded producer over the remote protocol must cross the
        // refill RPC and preserve the exact winners.
        let executor: Arc<dyn FragmentExecutor> = Arc::new(
            InMemoryFragmentExecutor::with_topk_emit_batch(Arc::clone(&store), 2),
        );
        let endpoint = Arc::new(RemoteFragmentEndpoint::new(executor));
        let client: Arc<dyn FragmentRpcClient> =
            Arc::new(LoopbackFragmentRpcClient::new(Arc::clone(&endpoint)));
        let coordinator = Coordinator::new(
            Arc::new(RemoteFragmentTransport::new(client)),
            Arc::new(SqlQueryRegistry::default()),
        );
        let plan = distribute(&desc(topk), &locator, &metadata).unwrap();
        let batches = coordinator.execute(&plan).await.unwrap();
        let actual = collect_u64(&batches, "score")
            .into_iter()
            .zip(collect_i64(&batches, "payload"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(endpoint.active_executions(), 0);
    }

    #[tokio::test]
    async fn cancellation_fans_out_to_every_fragment() {
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let executor = Arc::new(BlockingExecutor {
            barrier: Arc::clone(&barrier),
        });
        let (coordinator, transport, _registry) = coordinator_with(executor);
        let (plan, _) = scan_plan("t");
        let task = {
            let coordinator = Arc::clone(&coordinator);
            let plan = plan.clone();
            tokio::spawn(async move { coordinator.execute(&plan).await })
        };
        tokio::time::timeout(Duration::from_secs(30), barrier.wait())
            .await
            .expect("all fragments started");
        assert!(coordinator.cancel_query(&plan.query_id).unwrap());
        let result = task.await.unwrap();
        assert!(
            matches!(
                result,
                Err(DistributedError::Cancelled(
                    CancellationReason::ClientRequest
                ))
            ),
            "cancellation surfaces as ClientRequest: {result:?}"
        );
        let cancelled = transport.cancelled_fragments();
        for fragment_id in 0..3 {
            assert!(
                cancelled.contains(&fragment_id),
                "fragment {fragment_id} received the transport cancel: {cancelled:?}"
            );
            let control = transport.control_for(fragment_id).unwrap();
            assert_eq!(control.reason(), CancellationReason::ClientRequest);
        }
    }

    #[tokio::test]
    async fn remote_cancellation_reaches_worker_during_fragment_start() {
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let executor: Arc<dyn FragmentExecutor> = Arc::new(BlockingExecutor {
            barrier: Arc::clone(&barrier),
        });
        let endpoint = Arc::new(RemoteFragmentEndpoint::new(executor));
        let client: Arc<dyn FragmentRpcClient> =
            Arc::new(LoopbackFragmentRpcClient::new(Arc::clone(&endpoint)));
        let coordinator = Arc::new(Coordinator::new(
            Arc::new(RemoteFragmentTransport::new(client)),
            Arc::new(SqlQueryRegistry::default()),
        ));
        let (plan, _) = scan_plan("t");
        let task = {
            let coordinator = Arc::clone(&coordinator);
            let plan = plan.clone();
            tokio::spawn(async move { coordinator.execute(&plan).await })
        };
        tokio::time::timeout(Duration::from_secs(30), barrier.wait())
            .await
            .expect("all remote workers entered fragment start");
        assert!(coordinator.cancel_query(&plan.query_id).unwrap());
        let result = tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("remote cancellation must not hang")
            .unwrap();
        assert!(matches!(result, Err(DistributedError::Cancelled(_))));
        assert_eq!(endpoint.active_executions(), 0);
    }

    #[tokio::test]
    async fn registry_cancel_reaches_all_fragments() {
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let executor = Arc::new(BlockingExecutor {
            barrier: Arc::clone(&barrier),
        });
        let (coordinator, transport, registry) = coordinator_with(executor);
        let (plan, _) = scan_plan("t");
        let task = {
            let coordinator = Arc::clone(&coordinator);
            let plan = plan.clone();
            tokio::spawn(async move { coordinator.execute(&plan).await })
        };
        tokio::time::timeout(Duration::from_secs(30), barrier.wait())
            .await
            .expect("all fragments started");
        // The existing registry cancel path — this is the wiring proof.
        let bridged: crate::query_registry::QueryId = plan.query_id.to_hex().parse().unwrap();
        assert_eq!(registry.cancel(bridged), CancelOutcome::Accepted);
        let result = task.await.unwrap();
        assert!(
            matches!(result, Err(DistributedError::Cancelled(_))),
            "registry cancellation aborts the distributed query: {result:?}"
        );
        for fragment_id in 0..3 {
            let control = transport.control_for(fragment_id).unwrap();
            assert_eq!(
                control.reason(),
                CancellationReason::ClientRequest,
                "fragment {fragment_id} observed the registry cancel"
            );
        }
    }

    #[tokio::test]
    async fn lease_expiry_reclaims_abandoned_fragments() {
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let executor = Arc::new(BlockingExecutor {
            barrier: Arc::clone(&barrier),
        });
        let transport = Arc::new(InMemoryTransport::new(executor));
        let registry = Arc::new(SqlQueryRegistry::default());
        let coordinator = Arc::new(Coordinator::with_limits(
            Arc::clone(&transport) as Arc<dyn FragmentTransport>,
            registry,
            1_024,
            16 * 1024 * 1024 * 1024,
            Duration::from_secs(30),
        ));
        let (plan, _) = scan_plan("t");
        let task = {
            let coordinator = Arc::clone(&coordinator);
            let plan = plan.clone();
            tokio::spawn(async move { coordinator.execute(&plan).await })
        };
        tokio::time::timeout(Duration::from_secs(30), barrier.wait())
            .await
            .expect("all fragments started");
        assert_eq!(coordinator.resources().reserved_fragments(), 3);
        // All worker leases expired an hour from now's perspective.
        let cleaned = coordinator.sweep_expired_leases(Instant::now() + Duration::from_secs(3_600));
        assert_eq!(cleaned, 3, "every abandoned fragment is reclaimed");
        assert_eq!(
            coordinator.resources().reserved_fragments(),
            0,
            "reclaimed fragments release their reservations"
        );
        for fragment_id in 0..3 {
            let control = transport.control_for(fragment_id).unwrap();
            assert_eq!(
                control.reason(),
                CancellationReason::ServerShutdown,
                "fragment {fragment_id} observes the worker loss"
            );
        }
        let result = task.await.unwrap();
        assert!(
            matches!(
                result,
                Err(DistributedError::Cancelled(
                    CancellationReason::ServerShutdown
                ))
            ),
            "the query fails with the worker-loss reason: {result:?}"
        );
    }

    #[test]
    fn resource_reservation_denies_then_releases() {
        let ledger = Arc::new(ResourceLedger::new(1, u64::MAX));
        let fragment = PlanFragment {
            fragment_id: 0,
            assignment: FragmentAssignment::Coordinator,
            operators: Vec::new(),
            estimated_rows: 1,
            estimated_bytes: 100,
            max_spill_bytes: 0,
        };
        let permit = ledger.reserve(&fragment).unwrap();
        assert_eq!(ledger.reserved_fragments(), 1);
        assert_eq!(ledger.reserved_bytes(), 100);
        let denied = ledger.reserve(&fragment).unwrap_err();
        assert!(
            matches!(denied, DistributedError::Reservation { fragment_id: 0, .. }),
            "{denied}"
        );
        drop(permit);
        assert_eq!(ledger.reserved_fragments(), 0);
        assert_eq!(ledger.reserved_bytes(), 0);
        ledger.reserve(&fragment).unwrap();
        // Byte budget enforcement.
        let tight = Arc::new(ResourceLedger::new(8, 50));
        let denied = tight.reserve(&fragment).unwrap_err();
        assert!(
            matches!(denied, DistributedError::Reservation { .. }),
            "{denied}"
        );
    }

    #[test]
    fn repartition_routes_every_row_exactly_once() {
        let batch = i64_batch(&[("k", (0..100).collect()), ("v", (0..100).collect())]);
        let frames = vec![BatchFrame::data(batch)];
        let keys = vec!["k".to_owned()];
        let mut partitions = Vec::new();
        for index in 0..3 {
            partitions.push(repartition_frames(&frames, &keys, 3, index).unwrap());
        }
        let total: usize = partitions
            .iter()
            .map(|frames| {
                frames
                    .iter()
                    .map(|frame| frame.batch.num_rows())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(total, 100, "every row lands in exactly one partition");
        // Re-routing a partition keeps all of its rows in the same partition
        // (disjointness proof by idempotence).
        for (index, partition) in partitions.iter().enumerate() {
            for other in 0..3 {
                let rerouted = repartition_frames(partition, &keys, 3, other).unwrap();
                let rows: usize = rerouted.iter().map(|frame| frame.batch.num_rows()).sum();
                if other == index {
                    let expected: usize =
                        partition.iter().map(|frame| frame.batch.num_rows()).sum();
                    assert_eq!(rows, expected, "partition {index} rows stay in {index}");
                } else {
                    assert_eq!(rows, 0, "partition {index} leaks rows into {other}");
                }
            }
        }
    }

    #[test]
    fn score_key_mapping_preserves_order() {
        let ints = Int64Array::from(vec![i64::MIN, -5, 0, 5, i64::MAX]);
        let keys: Vec<u64> = (0..5).map(|row| score_key(&ints, row).unwrap()).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "int64 mapping is order-preserving");
        let floats = Float64Array::from(vec![f64::MIN, -1.5, 0.0, 2.5, f64::MAX]);
        let keys: Vec<u64> = (0..5).map(|row| score_key(&floats, row).unwrap()).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "float64 mapping is order-preserving");
    }

    // -----------------------------------------------------------------------
    // DataFusionDistributedPlanner (P0.4)
    // -----------------------------------------------------------------------

    fn df_table_source(
        columns: &[(&str, DataType)],
    ) -> Arc<dyn datafusion::logical_expr::TableSource> {
        use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
        let fields = columns
            .iter()
            .map(|(name, dtype)| Field::new(*name, dtype.clone(), true))
            .collect::<Vec<_>>();
        Arc::new(LogicalTableSource::new(Arc::new(Schema::new(fields))))
    }

    fn df_scan(table: &str, columns: &[(&str, DataType)]) -> datafusion::logical_expr::LogicalPlan {
        use datafusion::logical_expr::LogicalPlanBuilder;
        LogicalPlanBuilder::scan(table, df_table_source(columns), None)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn datafusion_planner_lowers_scan_filter_agg_sort_limit() {
        use datafusion::functions_aggregate::expr_fn::sum;
        use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};

        let plan = LogicalPlanBuilder::from(df_scan(
            "orders",
            &[("region", DataType::Utf8), ("amount", DataType::Int64)],
        ))
        .filter(col("amount").gt(lit(10i64)))
        .unwrap()
        .aggregate(vec![col("region")], vec![sum(col("amount"))])
        .unwrap()
        .sort(vec![col("region").sort(true, true)])
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

        let planner = DataFusionDistributedPlanner::new(QueryId::new_random());
        let lite = planner.to_lite(&plan).unwrap();
        match &lite {
            LogicalPlanLite::Limit { limit, input } => {
                assert_eq!(*limit, 5);
                match input.as_ref() {
                    LogicalPlanLite::Sort { keys, input, .. } => {
                        assert_eq!(keys.len(), 1);
                        assert!(!keys[0].descending);
                        match input.as_ref() {
                            LogicalPlanLite::Aggregate {
                                group_by,
                                aggregates,
                                input,
                            } => {
                                assert_eq!(group_by, &vec!["region".to_owned()]);
                                assert_eq!(aggregates.len(), 1);
                                assert_eq!(aggregates[0].function, AggregateFunction::Sum);
                                match input.as_ref() {
                                    LogicalPlanLite::Scan {
                                        table, predicate, ..
                                    } => {
                                        assert_eq!(table, "orders");
                                        assert!(
                                            predicate
                                                .as_ref()
                                                .is_some_and(|p| p.contains("amount")),
                                            "predicate={predicate:?}"
                                        );
                                    }
                                    other => panic!("expected scan under aggregate, got {other:?}"),
                                }
                            }
                            other => panic!("expected aggregate under sort, got {other:?}"),
                        }
                    }
                    other => panic!("expected sort under limit, got {other:?}"),
                }
            }
            other => panic!("expected limit root, got {other:?}"),
        }

        let t1 = TabletId::from_bytes([1; 16]);
        let t2 = TabletId::from_bytes([2; 16]);
        let (locator, metadata) = world(&[(
            "orders",
            &[t1, t2],
            hash_spec("region"),
            stats(1_000, 64_000),
        )]);
        let distributed = planner.lower(&plan, &locator, &metadata).unwrap();
        assert!(!distributed.fragments.is_empty());
        assert!(distributed.root_fragment_id().is_some());
        assert!(distributed.fragments.iter().any(|fragment| fragment
            .operators
            .iter()
            .any(|op| matches!(op, FragmentOperator::TabletScan { .. }))));
    }

    #[test]
    fn datafusion_planner_lowers_join_and_union() {
        use datafusion::logical_expr::{JoinType, LogicalPlanBuilder};

        let left = df_scan(
            "orders",
            &[("id", DataType::Int64), ("uid", DataType::Int64)],
        );
        let right = df_scan(
            "users",
            &[("id", DataType::Int64), ("name", DataType::Utf8)],
        );
        let join = LogicalPlanBuilder::from(left)
            .join(
                LogicalPlanBuilder::from(right).build().unwrap(),
                JoinType::Inner,
                (vec!["uid"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let planner = DataFusionDistributedPlanner::new(QueryId::new_random());
        let lite = planner.to_lite(&join).unwrap();
        match lite {
            LogicalPlanLite::Join { on, left, right } => {
                assert_eq!(on.len(), 1);
                assert_eq!(on[0].left, "uid");
                assert_eq!(on[0].right, "id");
                assert!(
                    matches!(left.as_ref(), LogicalPlanLite::Scan { table, .. } if table == "orders")
                );
                assert!(
                    matches!(right.as_ref(), LogicalPlanLite::Scan { table, .. } if table == "users")
                );
            }
            other => panic!("expected join, got {other:?}"),
        }

        let a = df_scan("a", &[("x", DataType::Int64)]);
        let b = df_scan("b", &[("x", DataType::Int64)]);
        let union = LogicalPlanBuilder::from(a)
            .union(LogicalPlanBuilder::from(b).build().unwrap())
            .unwrap()
            .build()
            .unwrap();
        let lite = planner.to_lite(&union).unwrap();
        match lite {
            LogicalPlanLite::Union { inputs } => {
                assert_eq!(inputs.len(), 2);
            }
            other => panic!("expected union, got {other:?}"),
        }

        let t = TabletId::from_bytes([9; 16]);
        let (locator, metadata) = world(&[
            ("a", &[t], PartitionSpec::Unpartitioned, stats(10, 100)),
            ("b", &[t], PartitionSpec::Unpartitioned, stats(10, 100)),
            ("orders", &[t], hash_spec("uid"), stats(100, 1_000)),
            ("users", &[t], hash_spec("id"), stats(100, 1_000)),
        ]);
        planner.lower(&join, &locator, &metadata).unwrap();
        planner.lower(&union, &locator, &metadata).unwrap();
    }

    #[test]
    fn datafusion_planner_rejects_unsupported_operators() {
        use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};

        let plan = LogicalPlanBuilder::from(df_scan("t", &[("v", DataType::Int64)]))
            .filter(col("v").gt(lit(1i64)))
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();
        let planner = DataFusionDistributedPlanner::new(QueryId::new_random());
        let err = planner.to_lite(&plan).unwrap_err();
        assert!(
            matches!(err, DistributedError::Unsupported(_)),
            "distinct must be rejected: {err:?}"
        );

        // VALUES is not a tablet scan and must be rejected.
        let values = LogicalPlanBuilder::values(vec![vec![lit(1i64)]])
            .unwrap()
            .build()
            .unwrap();
        let err = planner.to_lite(&values).unwrap_err();
        assert!(
            matches!(err, DistributedError::Unsupported(_)),
            "values must be rejected: {err:?}"
        );
    }

    #[test]
    fn logical_plan_lite_union_plans_to_coordinator_gather() {
        let t1 = TabletId::from_bytes([1; 16]);
        let t2 = TabletId::from_bytes([2; 16]);
        let (locator, metadata) = world(&[
            ("a", &[t1], PartitionSpec::Unpartitioned, stats(10, 100)),
            ("b", &[t2], PartitionSpec::Unpartitioned, stats(20, 200)),
        ]);
        let plan = distribute(
            &desc(LogicalPlanLite::Union {
                inputs: vec![scan("a"), scan("b")],
            }),
            &locator,
            &metadata,
        )
        .unwrap();
        assert!(plan.root_fragment_id().is_some());
        assert!(plan
            .fragments
            .iter()
            .any(|f| f.assignment == FragmentAssignment::Coordinator));
        assert_eq!(
            plan.fragments
                .iter()
                .filter(|f| matches!(f.assignment, FragmentAssignment::Tablet(_)))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn plan_sql_distributed_public_entry_lowers_real_sql() {
        // Public entry must exist and perform real DataFusion parse + lower.
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        let catalog = PlanningTableCatalog::new().with_table("orders", schema);
        let t1 = TabletId::from_bytes([1; 16]);
        let t2 = TabletId::from_bytes([2; 16]);
        let (locator, metadata) = world(&[(
            "orders",
            &[t1, t2],
            hash_spec("region"),
            stats(1_000, 64_000),
        )]);

        // Keep the SQL shape within the supported lower surface (scan+filter+
        // limit). Aggregate/join paths are covered by DataFusion plan unit tests.
        let plan = plan_sql_distributed(
            "SELECT region, amount FROM orders WHERE amount > 10 LIMIT 5",
            &catalog,
            &locator,
            &metadata,
        )
        .await
        .expect("plan_sql_distributed must lower real SQL");

        assert!(!plan.fragments.is_empty());
        assert!(plan.root_fragment_id().is_some());
        assert!(
            plan.fragments.iter().any(|fragment| fragment
                .operators
                .iter()
                .any(|op| matches!(op, FragmentOperator::TabletScan { table, .. } if table == "orders"))),
            "expected tablet scan of orders: {plan:?}"
        );
        assert_eq!(plan.metadata_version, metadata.metadata_version());

        // plan_logical_distributed is the same seam when a DF plan is already
        // available (e.g. from MongrelSession).
        let ctx = datafusion::prelude::SessionContext::new();
        let provider = datafusion::datasource::MemTable::try_new(
            Arc::clone(catalog.schema("orders").unwrap()),
            vec![vec![RecordBatch::new_empty(Arc::clone(
                catalog.schema("orders").unwrap(),
            ))]],
        )
        .unwrap();
        ctx.register_table("orders", Arc::new(provider)).unwrap();
        let df = ctx
            .sql("SELECT region FROM orders WHERE amount > 0")
            .await
            .unwrap();
        let from_logical =
            plan_logical_distributed(df.logical_plan(), &locator, &metadata).unwrap();
        assert!(!from_logical.fragments.is_empty());
    }

    #[tokio::test]
    async fn plan_sql_distributed_rejects_unknown_table() {
        let catalog = PlanningTableCatalog::new();
        let (locator, metadata) = world(&[]);
        let err = plan_sql_distributed("SELECT 1 FROM missing", &catalog, &locator, &metadata)
            .await
            .unwrap_err();
        assert!(
            matches!(err, DistributedError::InvalidPlan(_)),
            "unknown table must fail planning: {err:?}"
        );
    }
}
