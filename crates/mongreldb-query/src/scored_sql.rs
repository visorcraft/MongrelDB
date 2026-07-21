use arrow::array::{ArrayRef, Float32Array, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use mongreldb_core::query::{
    AnnCandidateDistance, AnnRerankRequest, Condition, Fusion, NamedRetriever, Retriever,
    RetrieverScore, SearchRequest, SetSimilarityRequest, VectorMetric,
};
use mongreldb_core::schema::{AnnQuantization, IndexKind};
use mongreldb_core::{Database, Principal, Schema, Table, TypeId, Value};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::query_registry::SqlTaskContext;

pub(crate) type TableMap = Arc<Mutex<HashMap<String, mongreldb_core::TableHandle>>>;

pub(crate) struct ScoredRuntime {
    timeout_ms: AtomicU64,
    max_work: AtomicUsize,
    max_fused_candidates: AtomicUsize,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl ScoredRuntime {
    pub(crate) fn from_env() -> Arc<Self> {
        let value = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value: &usize| *value > 0)
                .unwrap_or(default)
        };
        Arc::new(Self {
            timeout_ms: AtomicU64::new(value("MONGRELDB_SQL_AI_TIMEOUT_MS", 30_000) as u64),
            max_work: AtomicUsize::new(value("MONGRELDB_SQL_AI_MAX_WORK", 1_000_000)),
            max_fused_candidates: AtomicUsize::new(
                value(
                    "MONGRELDB_SQL_AI_MAX_FUSED_CANDIDATES",
                    mongreldb_core::query::MAX_FUSED_CANDIDATES,
                )
                .min(mongreldb_core::query::MAX_FUSED_CANDIDATES),
            ),
            semaphore: Arc::new(tokio::sync::Semaphore::new(value(
                "MONGRELDB_SQL_AI_MAX_CONCURRENT",
                4,
            ))),
        })
    }

    pub(crate) fn configure(
        &self,
        timeout: Duration,
        max_work: usize,
        max_fused_candidates: usize,
    ) -> mongreldb_core::Result<()> {
        if timeout.is_zero() || max_work == 0 || max_fused_candidates == 0 {
            return Err(mongreldb_core::MongrelError::InvalidArgument(
                "scored SQL limits must be greater than zero".into(),
            ));
        }
        if max_fused_candidates > mongreldb_core::query::MAX_FUSED_CANDIDATES {
            return Err(mongreldb_core::MongrelError::InvalidArgument(format!(
                "maximum fused candidates exceeds {}",
                mongreldb_core::query::MAX_FUSED_CANDIDATES
            )));
        }
        self.timeout_ms.store(
            timeout.as_millis().clamp(1, u64::MAX as u128) as u64,
            Ordering::Release,
        );
        self.max_work.store(max_work, Ordering::Release);
        self.max_fused_candidates
            .store(max_fused_candidates, Ordering::Release);
        Ok(())
    }

    fn execution(
        &self,
        parent: Option<&mongreldb_core::ExecutionControl>,
    ) -> mongreldb_core::query::AiExecutionContext {
        let timeout = Duration::from_millis(self.timeout_ms.load(Ordering::Acquire));
        let deadline = Some(std::time::Instant::now() + timeout);
        let control = parent
            .map(|parent| parent.child_with_deadline(deadline))
            .unwrap_or_else(|| mongreldb_core::ExecutionControl::new(deadline));
        mongreldb_core::query::AiExecutionContext::with_control(
            control,
            self.max_work.load(Ordering::Acquire),
            self.max_fused_candidates.load(Ordering::Acquire),
        )
    }
}

#[derive(serde::Deserialize)]
struct HybridSpec {
    #[serde(default)]
    must: Vec<HybridCondition>,
    retrievers: Vec<HybridNamedRetriever>,
    #[serde(default = "default_rrf_constant")]
    rrf_constant: u32,
    limit: usize,
}

fn default_rrf_constant() -> u32 {
    60
}

#[derive(serde::Deserialize)]
struct HybridNamedRetriever {
    name: String,
    #[serde(default = "default_weight")]
    weight: f64,
    #[serde(flatten)]
    retriever: HybridRetriever,
}

fn default_weight() -> f64 {
    1.0
}

impl HybridNamedRetriever {
    fn to_core(&self, schema: &Schema) -> DFResult<NamedRetriever> {
        Ok(NamedRetriever {
            name: self.name.clone(),
            weight: self.weight,
            retriever: self.retriever.to_core(schema)?,
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum HybridRetriever {
    Ann {
        column: String,
        query: Vec<f32>,
        k: usize,
    },
    Sparse {
        column: String,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    #[serde(rename = "minhash", alias = "min_hash")]
    MinHash {
        column: String,
        members: Vec<mongreldb_core::query::SetMember>,
        k: usize,
    },
}

impl HybridRetriever {
    fn to_core(&self, schema: &Schema) -> DFResult<Retriever> {
        Ok(match self {
            Self::Ann { column, query, k } => Retriever::Ann {
                column_id: column_id(schema, column)?,
                query: query.clone(),
                k: *k,
            },
            Self::Sparse { column, query, k } => Retriever::Sparse {
                column_id: column_id(schema, column)?,
                query: query.clone(),
                k: *k,
            },
            Self::MinHash { column, members, k } => Retriever::MinHash {
                column_id: column_id(schema, column)?,
                members: members.clone(),
                k: *k,
            },
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum HybridCondition {
    Pk {
        value: serde_json::Value,
    },
    BitmapEq {
        column: String,
        value: serde_json::Value,
    },
    BitmapIn {
        column: String,
        values: Vec<serde_json::Value>,
    },
    Range {
        column: String,
        lo: i64,
        hi: i64,
    },
    RangeF64 {
        column: String,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    FmContains {
        column: String,
        pattern: String,
    },
    FmContainsAll {
        column: String,
        patterns: Vec<String>,
    },
}

impl HybridCondition {
    fn to_core(&self, schema: &Schema) -> DFResult<Condition> {
        Ok(match self {
            Self::Pk { value } => {
                let primary_key = schema
                    .primary_key()
                    .ok_or_else(|| DataFusionError::Plan("table has no primary key".into()))?;
                Condition::Pk(json_value(value, &primary_key.ty)?.encode_key())
            }
            Self::BitmapEq { column, value } => {
                let column = schema
                    .column(column)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column}")))?;
                Condition::BitmapEq {
                    column_id: column.id,
                    value: json_value(value, &column.ty)?.encode_key(),
                }
            }
            Self::BitmapIn { column, values } => {
                let column = schema
                    .column(column)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column}")))?;
                Condition::BitmapIn {
                    column_id: column.id,
                    values: values
                        .iter()
                        .map(|value| json_value(value, &column.ty).map(|value| value.encode_key()))
                        .collect::<DFResult<_>>()?,
                }
            }
            Self::Range { column, lo, hi } => Condition::Range {
                column_id: column_id(schema, column)?,
                lo: *lo,
                hi: *hi,
            },
            Self::RangeF64 {
                column,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
            } => Condition::RangeF64 {
                column_id: column_id(schema, column)?,
                lo: *lo,
                lo_inclusive: *lo_inclusive,
                hi: *hi,
                hi_inclusive: *hi_inclusive,
            },
            Self::IsNull { column } => Condition::IsNull {
                column_id: column_id(schema, column)?,
            },
            Self::IsNotNull { column } => Condition::IsNotNull {
                column_id: column_id(schema, column)?,
            },
            Self::FmContains { column, pattern } => Condition::FmContains {
                column_id: column_id(schema, column)?,
                pattern: pattern.as_bytes().to_vec(),
            },
            Self::FmContainsAll { column, patterns } => Condition::FmContainsAll {
                column_id: column_id(schema, column)?,
                patterns: patterns
                    .iter()
                    .map(|pattern| pattern.as_bytes().to_vec())
                    .collect(),
            },
        })
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Ann,
    AnnExact,
    Sparse,
    MinHash,
    ExactSet,
    Hybrid,
}

struct ScoredFunction {
    kind: Kind,
    tables: TableMap,
    database: Option<Arc<Database>>,
    principal: Option<Principal>,
    principal_catalog_bound: bool,
    runtime: Arc<ScoredRuntime>,
}

type ScoredExecution =
    dyn Fn(&mongreldb_core::query::AiExecutionContext) -> DFResult<RecordBatch> + Send + Sync;

struct LiveScoredProvider {
    schema: SchemaRef,
    execute: Arc<ScoredExecution>,
    runtime: Arc<ScoredRuntime>,
}

struct LiveScoredExec {
    props: Arc<PlanProperties>,
    schema: SchemaRef,
    execute: Arc<ScoredExecution>,
    runtime: Arc<ScoredRuntime>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

struct CancelOnDrop(Option<mongreldb_core::query::AiExecutionContext>);

const CANCELLATION_GRACE: Duration = Duration::from_millis(100);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(context) = &self.0 {
            context.cancel();
        }
    }
}

impl fmt::Debug for LiveScoredProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveScoredProvider").finish_non_exhaustive()
    }
}

impl fmt::Debug for LiveScoredExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveScoredExec").finish_non_exhaustive()
    }
}

impl DisplayAs for LiveScoredExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LiveScoredExec")
    }
}

impl ExecutionPlan for LiveScoredExec {
    fn name(&self) -> &str {
        "LiveScoredExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "LiveScoredExec is a leaf node and has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LiveScoredExec is single-partition; invalid partition {partition}"
            )));
        }
        let query = task_context
            .session_config()
            .get_extension::<SqlTaskContext>()
            .map(|context| context.query().clone());
        let context = self
            .runtime
            .execution(query.as_ref().map(|query| query.control()));
        context
            .checkpoint()
            .map_err(|error| scored_control_error(query.as_ref(), error))?;
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let execute = Arc::clone(&self.execute);
        let semaphore = Arc::clone(&self.runtime.semaphore);
        let projection = self.projection.clone();
        let limit = self.limit;
        let item = async move {
            let mut cancel = CancelOnDrop(Some(context.clone()));
            let control = context.execution_control().clone();
            let permit = tokio::select! {
                permit = semaphore.acquire_owned() => permit.map_err(|_| {
                    DataFusionError::Execution("scored SQL cancelled".into())
                })?,
                _ = control.cancelled() => {
                    return Err(scored_control_error_from_context(query.as_ref(), &context));
                }
            };
            let worker_context = context.clone();
            let mut task = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                execute(&worker_context)
            });
            let result = tokio::select! {
                result = &mut task => result.map_err(|error| {
                    DataFusionError::Execution(format!("scored SQL worker failed: {error}"))
                })?,
                _ = control.cancelled() => {
                    if tokio::time::timeout(CANCELLATION_GRACE, &mut task).await.is_err() {
                        eprintln!("scored SQL worker exceeded cancellation grace");
                    }
                    return Err(scored_control_error_from_context(query.as_ref(), &context));
                }
            };
            cancel.0 = None;
            let mut batch = result?;
            if let Some(projection) = projection {
                batch = batch.project(&projection)?;
            }
            if let Some(limit) = limit {
                batch = batch.slice(0, limit.min(batch.num_rows()));
            }
            Ok(batch)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            futures::stream::once(item),
        )))
    }
}

fn scored_control_error(
    query: Option<&crate::RegisteredSqlQuery>,
    error: mongreldb_core::MongrelError,
) -> DataFusionError {
    if let Some(error) = query.and_then(|query| query.checkpoint().err()) {
        DataFusionError::External(Box::new(error))
    } else {
        DataFusionError::Execution(error.to_string())
    }
}

fn scored_control_error_from_context(
    query: Option<&crate::RegisteredSqlQuery>,
    context: &mongreldb_core::query::AiExecutionContext,
) -> DataFusionError {
    scored_control_error(
        query,
        context
            .checkpoint()
            .expect_err("cancelled scored context must fail its checkpoint"),
    )
}

#[async_trait::async_trait]
impl TableProvider for LiveScoredProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => Arc::clone(&self.schema),
        };
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(LiveScoredExec {
            props,
            schema,
            execute: Arc::clone(&self.execute),
            runtime: Arc::clone(&self.runtime),
            projection: projection.cloned(),
            limit,
        }))
    }
}

impl fmt::Debug for ScoredFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScoredFunction").finish_non_exhaustive()
    }
}

pub(crate) fn register(
    ctx: &SessionContext,
    tables: TableMap,
    database: Option<Arc<Database>>,
    principal: Option<Principal>,
    principal_catalog_bound: bool,
    runtime: Arc<ScoredRuntime>,
) {
    for (name, kind) in [
        ("ann_search_scored", Kind::Ann),
        ("ann_search_exact", Kind::AnnExact),
        ("sparse_search_scored", Kind::Sparse),
        ("minhash_search_scored", Kind::MinHash),
        ("set_similarity_scored", Kind::ExactSet),
        ("hybrid_search_scored", Kind::Hybrid),
    ] {
        ctx.register_udtf(
            name,
            Arc::new(ScoredFunction {
                kind,
                tables: Arc::clone(&tables),
                database: database.clone(),
                principal: principal.clone(),
                principal_catalog_bound,
                runtime: Arc::clone(&runtime),
            }),
        );
    }
}

fn live_provider(
    schema: SchemaRef,
    runtime: Arc<ScoredRuntime>,
    execute: impl Fn(&mongreldb_core::query::AiExecutionContext) -> DFResult<RecordBatch>
        + Send
        + Sync
        + 'static,
) -> Arc<dyn TableProvider> {
    Arc::new(LiveScoredProvider {
        schema,
        execute: Arc::new(execute),
        runtime,
    })
}

fn output_schema(schema: &Schema, projection: &[u16], extra: Vec<Field>) -> DFResult<SchemaRef> {
    let base = crate::arrow_conv::arrow_schema(&projected_schema(schema, projection))
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let mut fields = base
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend(extra);
    Ok(Arc::new(ArrowSchema::new(fields)))
}

#[allow(clippy::too_many_arguments)]
fn with_scored_read<T>(
    database: Option<&Arc<Database>>,
    handle: &mongreldb_core::TableHandle,
    table_name: &str,
    principal: Option<&Principal>,
    principal_catalog_bound: bool,
    required_columns: &[u16],
    context: &mongreldb_core::query::AiExecutionContext,
    mut read: impl FnMut(
        &Table,
        mongreldb_core::Snapshot,
        Option<&mongreldb_core::security::CandidateAuthorization<'_>>,
        Option<&Principal>,
    ) -> mongreldb_core::Result<T>,
) -> DFResult<T> {
    match database {
        Some(database) => {
            let authorization = mongreldb_core::ReadAuthorization {
                operation: mongreldb_core::ColumnOperation::Select,
                columns: required_columns.to_vec(),
                permissions: Vec::new(),
            };
            database
                .with_authorized_scored_read_context_at(
                    table_name,
                    principal,
                    principal_catalog_bound,
                    Some(&authorization),
                    Some(context),
                    None,
                    read,
                )
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        }
        None => {
            let (generation, snapshot) = handle
                .read_generation_with_context(Some(context))
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            context
                .checkpoint()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            read(generation.as_ref(), snapshot, None, principal)
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        }
    }
}

impl TableFunctionImpl for ScoredFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if matches!(self.kind, Kind::AnnExact) {
            return self.exact_ann_provider(args);
        }
        if matches!(self.kind, Kind::ExactSet) {
            return self.exact_set_provider(args);
        }
        if matches!(self.kind, Kind::Hybrid) {
            return self.hybrid_provider(args);
        }
        if args.len() != 5 {
            return Err(DataFusionError::Plan(
                "scored search requires table, column, JSON query, k, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let column_name = string_literal(&args[1])?;
        let query = string_literal(&args[2])?;
        let k = usize::try_from(integer_literal(&args[3])?)
            .ok()
            .filter(|k| *k > 0)
            .ok_or_else(|| DataFusionError::Plan("k must be > 0".into()))?;
        let projection_names: Vec<_> = string_literal(&args[4])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        if projection_names.len() > mongreldb_core::query::MAX_PROJECTION_COLUMNS {
            return Err(DataFusionError::Plan(format!(
                "projection exceeds {} columns",
                mongreldb_core::query::MAX_PROJECTION_COLUMNS
            )));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let column_id = schema
            .column(&column_name)
            .map(|column| column.id)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column_name}")))?;
        let projection: Vec<_> = projection_names
            .iter()
            .map(|name| {
                schema
                    .column(name)
                    .map(|column| column.id)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {name}")))
            })
            .collect::<DFResult<_>>()?;
        let mut required_columns = projection.clone();
        required_columns.push(column_id);
        if let Some(database) = &self.database {
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required_columns,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let retriever = parse_retriever(self.kind, column_id, &query, k)?;
        let ann_quantization = ann_quantization_for_column(&schema, column_id);
        let extra = match self.kind {
            Kind::Ann => {
                let mut fields = vec![Field::new("search_rank", DataType::UInt64, false)];
                fields.push(ann_score_field(ann_quantization));
                fields
            }
            Kind::Sparse => vec![
                Field::new("search_rank", DataType::UInt64, false),
                Field::new("sparse_score", DataType::Float32, false),
            ],
            Kind::MinHash => vec![
                Field::new("search_rank", DataType::UInt64, false),
                Field::new("estimated_jaccard", DataType::Float32, false),
            ],
            Kind::AnnExact | Kind::ExactSet | Kind::Hybrid => unreachable!(),
        };
        let provider_schema = output_schema(&schema, &projection, extra)?;
        let database = self.database.clone();
        let principal = self.principal.clone();
        let principal_catalog_bound = self.principal_catalog_bound;
        let kind = self.kind;
        let batch_schema = Arc::clone(&provider_schema);
        Ok(live_provider(
            provider_schema,
            Arc::clone(&self.runtime),
            move |context| {
                let (hits, rows) = with_scored_read(
                    database.as_ref(),
                    &handle,
                    &table_name,
                    principal.as_ref(),
                    principal_catalog_bound,
                    &required_columns,
                    context,
                    |table, snapshot, authorization, effective_principal| {
                        if let Some(database) = &database {
                            database.require_columns_for(
                                &table_name,
                                mongreldb_core::ColumnOperation::Select,
                                &required_columns,
                                effective_principal,
                            )?;
                        }
                        let hits = table.retrieve_at_with_candidate_authorization_on_generation(
                            &retriever,
                            snapshot,
                            authorization,
                            Some(context),
                        )?;
                        let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
                        let mut rows =
                            table.rows_for_rids_with_context(&row_ids, snapshot, context)?;
                        if let Some(database) = &database {
                            database.mask_rows_for(&table_name, &mut rows, effective_principal)?;
                        }
                        Ok((hits, rows))
                    },
                )?;
                let scores: HashMap<_, _> = hits
                    .into_iter()
                    .map(|hit| (hit.row_id, (hit.rank, hit.score)))
                    .collect();
                let rows: Vec<_> = rows
                    .into_iter()
                    .filter(|row| scores.contains_key(&row.row_id))
                    .collect();
                let projected = projected_schema(&schema, &projection);
                let base = crate::arrow_conv::rows_to_batch(&rows, &projected)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let ranks: Vec<_> = rows
                    .iter()
                    .map(|row| scores[&row.row_id].0 as u64)
                    .collect();
                let mut fields = base
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect::<Vec<_>>();
                let mut arrays = base.columns().to_vec();
                fields.push(Field::new("search_rank", DataType::UInt64, false));
                arrays.push(Arc::new(UInt64Array::from(ranks)) as ArrayRef);
                match kind {
                    Kind::Ann => {
                        append_ann_score_column(
                            ann_quantization,
                            &rows,
                            &scores,
                            &mut fields,
                            &mut arrays,
                        )?;
                    }
                    Kind::Sparse => {
                        append_float_score(
                            "sparse_score",
                            &rows,
                            &scores,
                            &mut fields,
                            &mut arrays,
                            |score| match score {
                                RetrieverScore::SparseDotProduct(score) => score,
                                _ => unreachable!(),
                            },
                        )?;
                    }
                    Kind::MinHash => {
                        append_float_score(
                            "estimated_jaccard",
                            &rows,
                            &scores,
                            &mut fields,
                            &mut arrays,
                            |score| match score {
                                RetrieverScore::MinHashEstimatedJaccard(score) => score as f64,
                                _ => unreachable!(),
                            },
                        )?;
                    }
                    Kind::AnnExact | Kind::ExactSet | Kind::Hybrid => unreachable!(),
                }
                RecordBatch::try_new(Arc::clone(&batch_schema), arrays)
                    .map_err(DataFusionError::from)
            },
        ))
    }
}

impl ScoredFunction {
    fn exact_ann_provider(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 7 {
            return Err(DataFusionError::Plan(
                "ann_search_exact requires table, column, JSON query, candidate_k, limit, metric, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let column_name = string_literal(&args[1])?;
        let query = serde_json::from_str(&string_literal(&args[2])?)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let candidate_k = positive_usize(&args[3], "candidate_k")?;
        let limit = positive_usize(&args[4], "limit")?;
        let metric = match string_literal(&args[5])?.to_ascii_lowercase().as_str() {
            "cosine" => VectorMetric::Cosine,
            "dot_product" | "dot" => VectorMetric::DotProduct,
            "euclidean" | "l2" => VectorMetric::Euclidean,
            metric => {
                return Err(DataFusionError::Plan(format!(
                    "unknown vector metric: {metric}"
                )))
            }
        };
        let projection_names = string_literal(&args[6])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        if projection_names.len() > mongreldb_core::query::MAX_PROJECTION_COLUMNS {
            return Err(DataFusionError::Plan(format!(
                "projection exceeds {} columns",
                mongreldb_core::query::MAX_PROJECTION_COLUMNS
            )));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let vector_column_id = column_id(&schema, &column_name)?;
        let projection = projection_names
            .iter()
            .map(|name| column_id(&schema, name))
            .collect::<DFResult<Vec<_>>>()?;
        let mut required_columns = projection.clone();
        required_columns.push(vector_column_id);
        if let Some(database) = &self.database {
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required_columns,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let request = AnnRerankRequest {
            column_id: vector_column_id,
            query,
            candidate_k,
            limit,
            metric,
        };
        let ann_quantization = ann_quantization_for_column(&schema, vector_column_id);
        let mut extra = vec![Field::new("search_rank", DataType::UInt64, false)];
        extra.push(exact_ann_candidate_field(ann_quantization));
        extra.push(Field::new("exact_score", DataType::Float32, false));
        let provider_schema = output_schema(&schema, &projection, extra)?;
        let database = self.database.clone();
        let principal = self.principal.clone();
        let principal_catalog_bound = self.principal_catalog_bound;
        let batch_schema = Arc::clone(&provider_schema);
        Ok(live_provider(
            provider_schema,
            Arc::clone(&self.runtime),
            move |context| {
                let (hits, rows) = with_scored_read(
                    database.as_ref(),
                    &handle,
                    &table_name,
                    principal.as_ref(),
                    principal_catalog_bound,
                    &required_columns,
                    context,
                    |table, snapshot, authorization, effective_principal| {
                        if let Some(database) = &database {
                            database.require_columns_for(
                                &table_name,
                                mongreldb_core::ColumnOperation::Select,
                                &required_columns,
                                effective_principal,
                            )?;
                        }
                        let hits = table.ann_rerank_at_with_candidate_authorization_on_generation(
                            &request,
                            snapshot,
                            authorization,
                            Some(context),
                        )?;
                        let row_ids = hits.iter().map(|hit| hit.row_id.0).collect::<Vec<_>>();
                        let mut rows =
                            table.rows_for_rids_with_context(&row_ids, snapshot, context)?;
                        if let Some(database) = &database {
                            database.mask_rows_for(&table_name, &mut rows, effective_principal)?;
                        }
                        Ok((hits, rows))
                    },
                )?;
                let scores = hits
                    .into_iter()
                    .enumerate()
                    .map(|(rank, hit)| {
                        (
                            hit.row_id,
                            (rank as u64 + 1, hit.candidate_distance, hit.exact_score),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                let mut rows = rows
                    .into_iter()
                    .filter(|row| scores.contains_key(&row.row_id))
                    .collect::<Vec<_>>();
                rows.sort_by_key(|row| scores[&row.row_id].0);
                let base = crate::arrow_conv::rows_to_batch(
                    &rows,
                    &projected_schema(&schema, &projection),
                )
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let mut arrays = base.columns().to_vec();
                arrays.push(Arc::new(UInt64Array::from(
                    rows.iter()
                        .map(|row| scores[&row.row_id].0)
                        .collect::<Vec<_>>(),
                )) as ArrayRef);
                match ann_quantization {
                    AnnQuantization::BinarySign => {
                        arrays.push(Arc::new(UInt32Array::from(
                            rows.iter()
                                .map(|row| match scores[&row.row_id].1 {
                                    AnnCandidateDistance::Hamming(distance) => distance,
                                    AnnCandidateDistance::Cosine(_) => unreachable!(
                                        "dense candidate distance on BinarySign ANN index"
                                    ),
                                })
                                .collect::<Vec<_>>(),
                        )) as ArrayRef);
                    }
                    // Dense and Product both report an f32 distance (cosine for
                    // Dense, ADC for Product). Product is gated behind
                    // validate_options until its backend lands.
                    AnnQuantization::Dense | AnnQuantization::Product { .. } => {
                        arrays.push(Arc::new(Float32Array::from(
                            rows.iter()
                                .map(|row| match scores[&row.row_id].1 {
                                    AnnCandidateDistance::Cosine(distance) => {
                                        if distance.is_finite() {
                                            Ok(distance)
                                        } else {
                                            Err(DataFusionError::Execution(
                                                "ann_cosine_distance must be finite".into(),
                                            ))
                                        }
                                    }
                                    AnnCandidateDistance::Hamming(_) => {
                                        Err(DataFusionError::Execution(
                                            "binary candidate distance on Dense ANN index".into(),
                                        ))
                                    }
                                })
                                .collect::<DFResult<Vec<_>>>()?,
                        )) as ArrayRef);
                    }
                }
                arrays.push(Arc::new(Float32Array::from(
                    rows.iter()
                        .map(|row| scores[&row.row_id].2)
                        .collect::<Vec<_>>(),
                )) as ArrayRef);
                RecordBatch::try_new(Arc::clone(&batch_schema), arrays)
                    .map_err(DataFusionError::from)
            },
        ))
    }

    fn exact_set_provider(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 7 {
            return Err(DataFusionError::Plan(
                "set_similarity_scored requires table, column, members, candidate_k, min_jaccard, limit, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let column_name = string_literal(&args[1])?;
        let members = serde_json::from_str(&string_literal(&args[2])?)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let candidate_k = positive_usize(&args[3], "candidate_k")?;
        let min_jaccard = float_literal(&args[4])? as f32;
        let limit = positive_usize(&args[5], "limit")?;
        let projection_names: Vec<_> = string_literal(&args[6])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        if projection_names.len() > mongreldb_core::query::MAX_PROJECTION_COLUMNS {
            return Err(DataFusionError::Plan(format!(
                "projection exceeds {} columns",
                mongreldb_core::query::MAX_PROJECTION_COLUMNS
            )));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let column_id = schema
            .column(&column_name)
            .map(|column| column.id)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column_name}")))?;
        let projection: Vec<_> = projection_names
            .iter()
            .map(|name| {
                schema
                    .column(name)
                    .map(|column| column.id)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {name}")))
            })
            .collect::<DFResult<_>>()?;
        let mut required_columns = projection.clone();
        required_columns.push(column_id);
        if let Some(database) = &self.database {
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required_columns,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let request = SetSimilarityRequest {
            column_id,
            members,
            candidate_k,
            min_jaccard,
            limit,
        };
        let provider_schema = output_schema(
            &schema,
            &projection,
            vec![
                Field::new("search_rank", DataType::UInt64, false),
                Field::new("estimated_jaccard", DataType::Float32, false),
                Field::new("exact_jaccard", DataType::Float32, false),
            ],
        )?;
        let database = self.database.clone();
        let principal = self.principal.clone();
        let principal_catalog_bound = self.principal_catalog_bound;
        let batch_schema = Arc::clone(&provider_schema);
        Ok(live_provider(
            provider_schema,
            Arc::clone(&self.runtime),
            move |context| {
                let (hits, rows) = with_scored_read(
                    database.as_ref(),
                    &handle,
                    &table_name,
                    principal.as_ref(),
                    principal_catalog_bound,
                    &required_columns,
                    context,
                    |table, snapshot, authorization, effective_principal| {
                        if let Some(database) = &database {
                            database.require_columns_for(
                                &table_name,
                                mongreldb_core::ColumnOperation::Select,
                                &required_columns,
                                effective_principal,
                            )?;
                        }
                        let hits = table
                            .set_similarity_at_with_candidate_authorization_on_generation(
                                &request,
                                snapshot,
                                authorization,
                                Some(context),
                            )?;
                        let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
                        let mut rows =
                            table.rows_for_rids_with_context(&row_ids, snapshot, context)?;
                        if let Some(database) = &database {
                            database.mask_rows_for(&table_name, &mut rows, effective_principal)?;
                        }
                        Ok((hits, rows))
                    },
                )?;
                let scores: HashMap<_, _> = hits
                    .into_iter()
                    .enumerate()
                    .map(|(rank, hit)| {
                        (
                            hit.row_id,
                            (rank as u64 + 1, hit.estimated_jaccard, hit.exact_jaccard),
                        )
                    })
                    .collect();
                let rows: Vec<_> = rows
                    .into_iter()
                    .filter(|row| scores.contains_key(&row.row_id))
                    .collect();
                let projected = projected_schema(&schema, &projection);
                let base = crate::arrow_conv::rows_to_batch(&rows, &projected)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let mut arrays = base.columns().to_vec();
                arrays.extend([
                    Arc::new(UInt64Array::from(
                        rows.iter()
                            .map(|row| scores[&row.row_id].0)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(Float32Array::from(
                        rows.iter()
                            .map(|row| scores[&row.row_id].1)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(Float32Array::from(
                        rows.iter()
                            .map(|row| scores[&row.row_id].2)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                ]);
                RecordBatch::try_new(Arc::clone(&batch_schema), arrays)
                    .map_err(DataFusionError::from)
            },
        ))
    }

    fn hybrid_provider(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 3 {
            return Err(DataFusionError::Plan(
                "hybrid_search_scored requires table, request JSON, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let spec: HybridSpec = serde_json::from_str(&string_literal(&args[1])?)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let projection_names: Vec<_> = string_literal(&args[2])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        if projection_names.len() > mongreldb_core::query::MAX_PROJECTION_COLUMNS {
            return Err(DataFusionError::Plan(format!(
                "projection exceeds {} columns",
                mongreldb_core::query::MAX_PROJECTION_COLUMNS
            )));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let projection: Vec<_> = projection_names
            .iter()
            .map(|name| column_id(&schema, name))
            .collect::<DFResult<_>>()?;
        let must: Vec<_> = spec
            .must
            .iter()
            .map(|condition| condition.to_core(&schema))
            .collect::<DFResult<_>>()?;
        let retrievers: Vec<_> = spec
            .retrievers
            .iter()
            .map(|retriever| retriever.to_core(&schema))
            .collect::<DFResult<_>>()?;
        let mut required_columns = projection.clone();
        required_columns.extend(mongreldb_core::query::condition_columns(&must));
        required_columns.extend(
            retrievers
                .iter()
                .map(|retriever| retriever.retriever.column_id()),
        );
        required_columns.sort_unstable();
        required_columns.dedup();
        if let Some(database) = &self.database {
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required_columns,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let request = SearchRequest {
            must,
            retrievers,
            fusion: Fusion::ReciprocalRank {
                constant: spec.rrf_constant,
            },
            rerank: None,
            limit: spec.limit,
            projection: Some(projection.clone()),
        };
        let provider_schema = output_schema(
            &schema,
            &projection,
            vec![
                Field::new("search_rank", DataType::UInt64, false),
                Field::new("fused_score", DataType::Float64, false),
                Field::new("components", DataType::Utf8, false),
            ],
        )?;
        let database = self.database.clone();
        let principal = self.principal.clone();
        let principal_catalog_bound = self.principal_catalog_bound;
        let batch_schema = Arc::clone(&provider_schema);
        Ok(live_provider(
            provider_schema,
            Arc::clone(&self.runtime),
            move |context| {
                let (hits, rows) = with_scored_read(
                    database.as_ref(),
                    &handle,
                    &table_name,
                    principal.as_ref(),
                    principal_catalog_bound,
                    &required_columns,
                    context,
                    |table, snapshot, authorization, effective_principal| {
                        if let Some(database) = &database {
                            database.require_columns_for(
                                &table_name,
                                mongreldb_core::ColumnOperation::Select,
                                &required_columns,
                                effective_principal,
                            )?;
                        }
                        let hits = table.search_at_with_candidate_authorization_on_generation(
                            &request,
                            snapshot,
                            authorization,
                            Some(context),
                        )?;
                        let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
                        let mut rows =
                            table.rows_for_rids_with_context(&row_ids, snapshot, context)?;
                        if let Some(database) = &database {
                            database.mask_rows_for(&table_name, &mut rows, effective_principal)?;
                        }
                        Ok((hits, rows))
                    },
                )?;
                let mut rows_by_id: HashMap<_, _> =
                    rows.into_iter().map(|row| (row.row_id, row)).collect();
                let mut output_rows = Vec::new();
                let mut ranks = Vec::new();
                let mut fused_scores = Vec::new();
                let mut component_json = Vec::new();
                for (rank, hit) in hits.into_iter().enumerate() {
                    let Some(row) = rows_by_id.remove(&hit.row_id) else {
                        continue;
                    };
                    output_rows.push(row);
                    ranks.push(rank as u64 + 1);
                    fused_scores.push(hit.fused_score);
                    component_json.push(
                        serde_json::to_string(
                            &hit.components
                                .into_iter()
                                .map(|component| {
                                    serde_json::json!({
                                        "retriever_name": component.retriever_name,
                                        "rank": component.rank,
                                        "raw_score": score_json(component.raw_score),
                                        "contribution": component.contribution,
                                    })
                                })
                                .collect::<Vec<_>>(),
                        )
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?,
                    );
                }
                let projected = projected_schema(&schema, &projection);
                let base = crate::arrow_conv::rows_to_batch(&output_rows, &projected)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let mut arrays = base.columns().to_vec();
                arrays.extend([
                    Arc::new(UInt64Array::from(ranks)) as ArrayRef,
                    Arc::new(Float64Array::from(fused_scores)) as ArrayRef,
                    Arc::new(StringArray::from(component_json)) as ArrayRef,
                ]);
                RecordBatch::try_new(Arc::clone(&batch_schema), arrays)
                    .map_err(DataFusionError::from)
            },
        ))
    }
}

fn append_float_score(
    name: &str,
    rows: &[mongreldb_core::Row],
    scores: &HashMap<mongreldb_core::RowId, (usize, RetrieverScore)>,
    fields: &mut Vec<Field>,
    arrays: &mut Vec<ArrayRef>,
    value: impl Fn(RetrieverScore) -> f64,
) -> DFResult<()> {
    let values = rows
        .iter()
        .map(|row| {
            let value = value(scores[&row.row_id].1) as f32;
            value.is_finite().then_some(value).ok_or_else(|| {
                DataFusionError::Execution(format!("{name} exceeds finite f32 range"))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;
    fields.push(Field::new(name, DataType::Float32, false));
    arrays.push(Arc::new(Float32Array::from(values)));
    Ok(())
}

/// Resolve ANN quantization for `column_id` from the authoritative schema.
/// Missing ANN options default to BinarySign (engine default).
pub(crate) fn ann_quantization_for_column(schema: &Schema, column_id: u16) -> AnnQuantization {
    schema
        .indexes
        .iter()
        .find(|index| index.column_id == column_id && index.kind == IndexKind::Ann)
        .and_then(|index| index.options.ann.as_ref())
        .map(|options| options.quantization)
        .unwrap_or(AnnQuantization::BinarySign)
}

fn ann_score_field(quantization: AnnQuantization) -> Field {
    match quantization {
        AnnQuantization::BinarySign => Field::new("ann_distance", DataType::UInt32, false),
        AnnQuantization::Dense => Field::new("ann_cosine_distance", DataType::Float32, false),
        // Product quantization reports an ADC (asymmetric distance computation)
        // distance as f32. Field is forward-ready; PQ is gated behind
        // validate_options until the backend lands.
        AnnQuantization::Product { .. } => Field::new("ann_distance", DataType::Float32, false),
    }
}

fn exact_ann_candidate_field(quantization: AnnQuantization) -> Field {
    match quantization {
        // Binary exact path keeps the historical candidate column name.
        AnnQuantization::BinarySign => Field::new("hamming_distance", DataType::UInt32, false),
        AnnQuantization::Dense => Field::new("ann_cosine_distance", DataType::Float32, false),
        AnnQuantization::Product { .. } => Field::new("ann_distance", DataType::Float32, false),
    }
}

fn append_ann_score_column(
    quantization: AnnQuantization,
    rows: &[mongreldb_core::Row],
    scores: &HashMap<mongreldb_core::RowId, (usize, RetrieverScore)>,
    fields: &mut Vec<Field>,
    arrays: &mut Vec<ArrayRef>,
) -> DFResult<()> {
    match quantization {
        AnnQuantization::BinarySign => {
            fields.push(Field::new("ann_distance", DataType::UInt32, false));
            arrays.push(Arc::new(UInt32Array::from(
                rows.iter()
                    .map(|row| match scores[&row.row_id].1 {
                        RetrieverScore::AnnHammingDistance(score) => score,
                        RetrieverScore::AnnCosineDistance(_) => {
                            unreachable!("dense score on BinarySign ANN index")
                        }
                        _ => unreachable!("non-ANN score for ann_search_scored"),
                    })
                    .collect::<Vec<_>>(),
            )));
            Ok(())
        }
        AnnQuantization::Dense => {
            let values = rows
                .iter()
                .map(|row| match scores[&row.row_id].1 {
                    RetrieverScore::AnnCosineDistance(score) => {
                        score.is_finite().then_some(score).ok_or_else(|| {
                            DataFusionError::Execution(
                                "ann_cosine_distance exceeds finite f32 range".into(),
                            )
                        })
                    }
                    RetrieverScore::AnnHammingDistance(_) => Err(DataFusionError::Execution(
                        "binary Hamming score on Dense ANN index".into(),
                    )),
                    _ => Err(DataFusionError::Execution(
                        "non-ANN score for ann_search_scored".into(),
                    )),
                })
                .collect::<DFResult<Vec<_>>>()?;
            fields.push(Field::new("ann_cosine_distance", DataType::Float32, false));
            arrays.push(Arc::new(Float32Array::from(values)));
            Ok(())
        }
        // Product quantization reports an f32 ADC distance. Gated behind
        // validate_options until the PQ backend lands; the field shape is
        // forward-ready.
        AnnQuantization::Product { .. } => {
            let values = rows
                .iter()
                .map(|row| match scores[&row.row_id].1 {
                    RetrieverScore::AnnCosineDistance(score) => {
                        score.is_finite().then_some(score).ok_or_else(|| {
                            DataFusionError::Execution(
                                "ann_distance exceeds finite f32 range".into(),
                            )
                        })
                    }
                    _ => Err(DataFusionError::Execution(
                        "non-ADC score for Product ANN index".into(),
                    )),
                })
                .collect::<DFResult<Vec<_>>>()?;
            fields.push(Field::new("ann_distance", DataType::Float32, false));
            arrays.push(Arc::new(Float32Array::from(values)));
            Ok(())
        }
    }
}

fn parse_retriever(kind: Kind, column_id: u16, query: &str, k: usize) -> DFResult<Retriever> {
    Ok(match kind {
        Kind::Ann => Retriever::Ann {
            column_id,
            query: serde_json::from_str(query)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            k,
        },
        Kind::AnnExact => unreachable!(),
        Kind::Sparse => Retriever::Sparse {
            column_id,
            query: serde_json::from_str(query)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            k,
        },
        Kind::MinHash => Retriever::MinHash {
            column_id,
            members: serde_json::from_str(query)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            k,
        },
        Kind::ExactSet => unreachable!(),
        Kind::Hybrid => unreachable!(),
    })
}

fn projected_schema(schema: &Schema, projection: &[u16]) -> Schema {
    Schema {
        schema_id: schema.schema_id,
        columns: projection
            .iter()
            .filter_map(|id| {
                schema
                    .columns
                    .iter()
                    .find(|column| column.id == *id)
                    .cloned()
            })
            .collect(),
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn column_id(schema: &Schema, name: &str) -> DFResult<u16> {
    schema
        .column(name)
        .map(|column| column.id)
        .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {name}")))
}

fn json_value(value: &serde_json::Value, ty: &TypeId) -> DFResult<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match ty {
        TypeId::Bool => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| DataFusionError::Plan("expected boolean value".into())),
        TypeId::Int8
        | TypeId::Int16
        | TypeId::Int32
        | TypeId::Int64
        | TypeId::UInt8
        | TypeId::UInt16
        | TypeId::UInt32
        | TypeId::UInt64
        | TypeId::TimestampNanos
        | TypeId::Date32
        | TypeId::Date64
        | TypeId::Time64 => value
            .as_i64()
            .map(Value::Int64)
            .ok_or_else(|| DataFusionError::Plan("expected integer value".into())),
        TypeId::Float32 | TypeId::Float64 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Value::Float64)
            .ok_or_else(|| DataFusionError::Plan("expected finite number".into())),
        TypeId::Bytes | TypeId::Enum { .. } => value
            .as_str()
            .map(|value| Value::Bytes(value.as_bytes().to_vec()))
            .ok_or_else(|| DataFusionError::Plan("expected string value".into())),
        TypeId::Embedding { dim } => {
            let values = value
                .as_array()
                .filter(|values| values.len() == *dim as usize)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("expected embedding dimension {dim}"))
                })?;
            let values = values
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .map(|value| value as f32)
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| {
                            DataFusionError::Plan("expected finite embedding value".into())
                        })
                })
                .collect::<DFResult<_>>()?;
            Ok(Value::Embedding(values))
        }
        TypeId::Json | TypeId::Array { .. } => serde_json::to_vec(value)
            .map(Value::Json)
            .map_err(|error| DataFusionError::Plan(error.to_string())),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported SQL search value type: {ty:?}"
        ))),
    }
}

fn score_json(score: RetrieverScore) -> serde_json::Value {
    match score {
        RetrieverScore::AnnHammingDistance(value) => {
            serde_json::json!({"kind":"ann_hamming_distance","value":value})
        }
        RetrieverScore::AnnCosineDistance(value) => {
            serde_json::json!({"kind":"ann_cosine_distance","value":value})
        }
        RetrieverScore::SparseDotProduct(value) => {
            serde_json::json!({"kind":"sparse_dot_product","value":value})
        }
        RetrieverScore::MinHashEstimatedJaccard(value) => {
            serde_json::json!({"kind":"minhash_estimated_jaccard","value":value})
        }
    }
}

fn positive_usize(expr: &Expr, name: &str) -> DFResult<usize> {
    usize::try_from(integer_literal(expr)?)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| DataFusionError::Plan(format!("{name} must be > 0")))
}

fn float_literal(expr: &Expr) -> DFResult<f64> {
    match expr {
        Expr::Literal(ScalarValue::Float64(Some(value)), _) => Ok(*value),
        Expr::Literal(ScalarValue::Float32(Some(value)), _) => Ok(*value as f64),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value as f64),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Ok(*value as f64),
        _ => Err(DataFusionError::Plan(
            "min_jaccard must be a numeric literal".into(),
        )),
    }
}

fn string_literal(expr: &Expr) -> DFResult<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)),
            _,
        ) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(
            "scored search arguments must be literals".into(),
        )),
    }
}

fn integer_literal(expr: &Expr) -> DFResult<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Ok(*value as i64),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => {
            i64::try_from(*value).map_err(|_| DataFusionError::Plan("k is too large".into()))
        }
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Ok(*value as i64),
        _ => Err(DataFusionError::Plan("k must be an integer literal".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_core::schema::{
        AnnOptions, ColumnDef, ColumnFlags, IndexDef, IndexOptions, Schema,
    };
    use mongreldb_core::TypeId;

    fn schema_with_ann(quantization: AnnQuantization) -> Schema {
        Schema {
            schema_id: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 2,
                    name: "embedding".into(),
                    ty: TypeId::Embedding { dim: 8 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: IndexOptions {
                    ann: Some(AnnOptions {
                        quantization,
                        ..AnnOptions::default()
                    }),
                    ..IndexOptions::default()
                },
            }],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    #[test]
    fn scored_sql_column_choice_follows_ann_quantization() {
        assert_eq!(
            ann_score_field(AnnQuantization::BinarySign).name(),
            "ann_distance"
        );
        assert_eq!(
            ann_score_field(AnnQuantization::BinarySign).data_type(),
            &DataType::UInt32
        );
        assert_eq!(
            ann_score_field(AnnQuantization::Dense).name(),
            "ann_cosine_distance"
        );
        assert_eq!(
            ann_score_field(AnnQuantization::Dense).data_type(),
            &DataType::Float32
        );
        assert_eq!(
            exact_ann_candidate_field(AnnQuantization::BinarySign).name(),
            "hamming_distance"
        );
        assert_eq!(
            exact_ann_candidate_field(AnnQuantization::Dense).name(),
            "ann_cosine_distance"
        );
    }

    #[test]
    fn ann_quantization_selected_from_index_schema() {
        let binary = schema_with_ann(AnnQuantization::BinarySign);
        let dense = schema_with_ann(AnnQuantization::Dense);
        assert_eq!(
            ann_quantization_for_column(&binary, 2),
            AnnQuantization::BinarySign
        );
        assert_eq!(
            ann_quantization_for_column(&dense, 2),
            AnnQuantization::Dense
        );
        assert_eq!(
            ann_quantization_for_column(&dense, 99),
            AnnQuantization::BinarySign
        );
    }

    #[test]
    fn score_json_uses_distinct_ann_kinds() {
        let hamming = score_json(RetrieverScore::AnnHammingDistance(3));
        assert_eq!(hamming["kind"], "ann_hamming_distance");
        assert_eq!(hamming["value"], 3);
        let cosine = score_json(RetrieverScore::AnnCosineDistance(0.25));
        assert_eq!(cosine["kind"], "ann_cosine_distance");
        assert!((cosine["value"].as_f64().unwrap() - 0.25).abs() < 1e-6);
        // Dense must never be cast into the Hamming kind/name.
        assert_ne!(cosine["kind"], "ann_hamming_distance");
    }
}
