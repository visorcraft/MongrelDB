//! Streaming page-aware scan execution plan (Phase 6.1 + 6.2).
//!
//! [`MongrelScanExec`] is a leaf DataFusion `ExecutionPlan` with three sources:
//!
//! * **Rows** — materialized visible columns, chunked into one `RecordBatch` per
//!   [`PAGE_BATCH_ROWS`] rows, lazily converted (the multi-run / non-empty
//!   memtable fallback, and the `COUNT(*)` zero-column path).
//! * **Cursor** — a core [`NativePageCursor`] for the single-run fast path that
//!   skips pages with no survivors and decodes only the projected columns of
//!   surviving pages lazily (fused predicate + page skip + late materialization;
//!   a `LIMIT` short-circuits page decode).
//!
//! In every mode DataFusion pipelines filter → aggregate → join → limit across
//! small batches, so peak Arrow memory stays bounded and a satisfied `LIMIT`
//! never pays for the rest.

use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures::stream;
use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::schema::TypeId;
use mongreldb_core::Cursor;
use mongreldb_core::{ColumnStat, Value};

use crate::arrow_conv::native_to_array_owned_with_query;
use crate::error::MongrelQueryError;
use crate::query_registry::{RegisteredSqlQuery, SqlTaskContext};

/// Rows per streamed `RecordBatch`. Matches the encoded 65 536-row page size so
/// a single batch typically corresponds to exactly one on-disk page.
pub(crate) const PAGE_BATCH_ROWS: usize = 65_536;

/// Backing data for a [`MongrelScanExec`].
enum Source {
    /// Fully materialized columns (multi-run/memtable fallback) — chunked.
    Rows {
        columns: Arc<Vec<NativeColumn>>,
        total_rows: usize,
    },
    /// A lazy page-aware cursor (single-run fast path or the Phase 16.1
    /// multi-run k-way merge) — one batch per surviving page / merge chunk.
    /// Boxed: the cursor owns large `RunReader`s. Wrapped in a `Mutex` so
    /// `execute(&self)` can extract it exactly once.
    Cursor(Box<Mutex<Option<Box<dyn Cursor>>>>),
    /// A pre-built Arrow `RecordBatch` (Phase 15.5: zero-copy from the Arrow
    /// IPC shadow). Streamed as-is (no per-column decode).
    Batch(RecordBatch),
}

/// A leaf `ExecutionPlan` that streams a MongrelDB table in fixed-size / page
/// chunks. See the module docs for the three source modes. It also reports
/// exact `num_rows` (and, for insert-only tables, per-column min/max) via
/// [`ExecutionPlan::partition_statistics`], so DataFusion's `AggregateStatistics`
/// rule answers `COUNT(*)`/`MIN`/`MAX` without scanning.
pub(crate) struct MongrelScanExec {
    props: Arc<PlanProperties>,
    schema: SchemaRef,
    types: Arc<Vec<TypeId>>,
    source: Source,
    /// Exact output row count of this scan.
    num_rows: usize,
    /// Per-column stats in output-field order (exact only where populated).
    column_stats: Arc<Vec<ColumnStatistics>>,
    /// Phase 16.3a: optional residual predicate (LIKE on Bytes).
    residual: Option<Arc<ResidualFilter>>,
}

impl MongrelScanExec {
    /// Materialized-columns scan: `columns` ordered to match `schema`'s fields,
    /// with `types[i]` the [`TypeId`] of `columns[i]`. All columns same length.
    pub(crate) fn new(
        schema: SchemaRef,
        columns: Vec<NativeColumn>,
        types: Vec<TypeId>,
        num_rows: usize,
        column_stats: Vec<ColumnStatistics>,
    ) -> Self {
        Self::rows(
            schema,
            Arc::new(types),
            Arc::new(columns),
            num_rows,
            Arc::new(column_stats),
        )
    }

    /// Zero-column scan that reports `total_rows` via empty-schema batches
    /// (the `COUNT(*)` path).
    pub(crate) fn new_row_count(total_rows: usize) -> Self {
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        Self::rows(
            schema,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            total_rows,
            Arc::new(Vec::new()),
        )
    }

    /// Cursor-backed scan for the single-run fast path or the Phase 16.1
    /// multi-run streaming path. `types` must match the cursor's projection
    /// order; `num_rows` is the exact survivor count.
    pub(crate) fn new_cursor(
        schema: SchemaRef,
        types: Vec<TypeId>,
        cursor: Box<dyn Cursor>,
        num_rows: usize,
        column_stats: Vec<ColumnStatistics>,
        residual: Option<Arc<ResidualFilter>>,
    ) -> Self {
        Self {
            props: make_props(&schema),
            schema,
            types: Arc::new(types),
            source: Source::Cursor(Box::new(Mutex::new(Some(cursor)))),
            num_rows,
            column_stats: Arc::new(column_stats),
            residual,
        }
    }

    /// Pre-built `RecordBatch` scan (Phase 15.5: zero-copy from the Arrow IPC
    /// shadow). The batch must match `schema`.
    pub(crate) fn new_batch(
        schema: SchemaRef,
        batch: RecordBatch,
        column_stats: Vec<ColumnStatistics>,
    ) -> Self {
        let num_rows = batch.num_rows();
        Self {
            props: make_props(&schema),
            types: Arc::new(Vec::new()),
            schema,
            source: Source::Batch(batch),
            num_rows,
            column_stats: Arc::new(column_stats),
            residual: None,
        }
    }

    fn rows(
        schema: SchemaRef,
        types: Arc<Vec<TypeId>>,
        columns: Arc<Vec<NativeColumn>>,
        total_rows: usize,
        column_stats: Arc<Vec<ColumnStatistics>>,
    ) -> Self {
        Self {
            props: make_props(&schema),
            schema,
            types,
            source: Source::Rows {
                columns,
                total_rows,
            },
            num_rows: total_rows,
            column_stats,
            residual: None,
        }
    }
}

/// Build a `ColumnStatistics` from an optional exact [`ColumnStat`]. Absent
/// stats (or an all-null column with no min/max) yield an all-`Absent` entry so
/// DataFusion falls back to computing the aggregate by scanning.
pub(crate) fn to_col_statistics(stat: Option<&ColumnStat>) -> ColumnStatistics {
    match stat {
        Some(s) => {
            let min = s.min.as_ref().map(value_to_scalar).map(Precision::Exact);
            let max = s.max.as_ref().map(value_to_scalar).map(Precision::Exact);
            ColumnStatistics {
                null_count: Precision::Exact(s.null_count as usize),
                min_value: min.unwrap_or(Precision::Absent),
                max_value: max.unwrap_or(Precision::Absent),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }
        }
        None => ColumnStatistics::new_unknown(),
    }
}

/// Map a MongrelDB [`Value`] to the matching Arrow [`ScalarValue`] for stats.
fn value_to_scalar(v: &Value) -> ScalarValue {
    match v {
        Value::Int64(x) => ScalarValue::Int64(Some(*x)),
        Value::Float64(x) => ScalarValue::Float64(Some(*x)),
        Value::Bytes(b) => ScalarValue::Utf8(Some(String::from_utf8_lossy(b).into_owned())),
        _ => ScalarValue::Null,
    }
}

fn make_props(schema: &SchemaRef) -> Arc<PlanProperties> {
    let eq = EquivalenceProperties::new(schema.clone());
    Arc::new(PlanProperties::new(
        eq,
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

impl fmt::Debug for MongrelScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongrelScanExec")
            .field("mode", &self.source)
            .field("batch_rows", &PAGE_BATCH_ROWS)
            .finish()
    }
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Rows { total_rows, .. } => write!(f, "rows({total_rows})"),
            Source::Cursor(_) => write!(f, "cursor"),
            Source::Batch(b) => write!(f, "batch({})", b.num_rows()),
        }
    }
}

impl DisplayAs for MongrelScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MongrelScanExec: mode={:?}, batch_rows={PAGE_BATCH_ROWS}",
            self.source
        )
    }
}

impl ExecutionPlan for MongrelScanExec {
    fn name(&self) -> &str {
        "MongrelScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    /// Exact `num_rows` (the scan's true output count) and, for insert-only
    /// tables, per-column min/max/null_count — so DataFusion's
    /// `AggregateStatistics` rule answers `COUNT(*)`/`MIN`/`MAX` without
    /// executing the scan.
    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::common::Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Exact(self.num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: (*self.column_stats).clone(),
        }))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            // A leaf rewritten with no children is a no-op clone (DataFusion's
            // FilterPushdown / repartition rules exercise this path).
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "MongrelScanExec is a leaf node and has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "MongrelScanExec is single-partition; invalid partition {partition}"
            )));
        }
        let query = ctx
            .session_config()
            .get_extension::<SqlTaskContext>()
            .map(|context| context.query().clone());
        checkpoint(query.as_ref())?;
        match &self.source {
            Source::Rows {
                columns,
                total_rows,
            } => {
                let total = *total_rows;
                if total == 0 {
                    return Ok(Box::pin(RecordBatchStreamAdapter::new(
                        self.schema.clone(),
                        stream::empty(),
                    )));
                }
                let columns = Arc::clone(columns);
                let types = Arc::clone(&self.types);
                let schema = self.schema.clone();
                let num_chunks = total.div_ceil(PAGE_BATCH_ROWS);
                let batch_schema = schema.clone();
                let query = query.clone();
                // Lazily build one batch per chunk: the iterator is only
                // advanced as DataFusion polls, so a LIMIT satisfied early
                // never pays the Arrow-conversion cost of later chunks.
                let chunk_iter = (0..num_chunks).map(move |i| {
                    checkpoint(query.as_ref())?;
                    let start = i * PAGE_BATCH_ROWS;
                    let end = (start + PAGE_BATCH_ROWS).min(total);
                    if columns.is_empty() {
                        build_row_count_batch(&batch_schema, end - start)
                    } else {
                        build_chunk_batch(
                            &columns,
                            &types,
                            &batch_schema,
                            start,
                            end,
                            query.as_ref(),
                        )
                    }
                });
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(chunk_iter),
                )))
            }
            Source::Cursor(mtx) => {
                // Single-partition ⇒ execute is called once. Extract the cursor.
                let cursor = mtx
                    .lock()
                    .expect("cursor mutex poisoned")
                    .take()
                    .ok_or_else(|| {
                        DataFusionError::Internal("MongrelScanExec cursor already consumed".into())
                    })?;
                let batches = CursorBatches {
                    cursor: Some(cursor),
                    types: Arc::clone(&self.types),
                    schema: self.schema.clone(),
                    residual: self.residual.clone(),
                    query,
                };
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    stream::iter(batches),
                )))
            }
            Source::Batch(batch) => {
                let schema = self.schema.clone();
                let batch = batch.clone();
                let item = checkpoint(query.as_ref()).map(|()| batch);
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(std::iter::once(item)),
                )))
            }
        }
    }
}

/// Phase 16.3a: a residual predicate applied to `NativeColumn` buffers before
/// Arrow conversion, avoiding per-row Arrow allocation for non-matching rows.
/// Currently only `BytesLike` (the FM-pushdown Inexact case — DataFusion would
/// otherwise re-apply the LIKE on the full RecordBatch).
pub(crate) struct ResidualFilter {
    col_idx: usize,
    pattern: Vec<u8>,
}

impl ResidualFilter {
    pub(crate) fn new(col_idx: usize, pattern: Vec<u8>) -> Self {
        Self { col_idx, pattern }
    }
    /// Apply the filter to a decoded column batch in-place (gather survivors).
    pub(crate) fn apply_with_query(
        &self,
        cols: &mut [NativeColumn],
        query: Option<&RegisteredSqlQuery>,
    ) -> datafusion::common::Result<()> {
        let Some(col) = cols.get(self.col_idx) else {
            return Ok(());
        };
        let n = col.len();
        let mut indices = Vec::with_capacity(n);
        for i in 0..n {
            if i % 256 == 0 {
                checkpoint(query)?;
            }
            if match col {
                NativeColumn::Bytes {
                    offsets, values, ..
                } => {
                    let lo = offsets[i] as usize;
                    let hi = offsets[i + 1] as usize;
                    like_match(&self.pattern, &values[lo..hi])
                }
                _ => true,
            } {
                indices.push(i);
            }
        }
        if indices.len() == n {
            return Ok(()); // All rows match — no gather needed.
        }
        for col in cols.iter_mut() {
            checkpoint(query)?;
            *col = col.gather(&indices);
        }
        Ok(())
    }
}

/// SQL LIKE pattern matching: `%` = any sequence, `_` = single char.
fn like_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0usize;
    let mut t = 0usize;
    let mut star_p: Option<usize> = None;
    let mut star_t = 0usize;
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'_' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'%' {
            star_p = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'%' {
        p += 1;
    }
    p == pattern.len()
}

/// Iterator adapter that pulls page batches from a [`NativePageCursor`] and
/// converts each to a `RecordBatch`. Yielded lazily by `stream::iter`, so pages
/// are decoded only as DataFusion pulls.
struct CursorBatches {
    cursor: Option<Box<dyn Cursor>>,
    types: Arc<Vec<TypeId>>,
    schema: SchemaRef,
    residual: Option<Arc<ResidualFilter>>,
    query: Option<RegisteredSqlQuery>,
}

impl Iterator for CursorBatches {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(error) = checkpoint(self.query.as_ref()) {
            self.cursor = None;
            return Some(Err(error));
        }
        let cursor = self.cursor.as_mut()?;
        match cursor.next_batch() {
            Ok(Some(mut cols)) => {
                if let Err(error) = checkpoint(self.query.as_ref()) {
                    self.cursor = None;
                    return Some(Err(error));
                }
                // Phase 16.3a: apply residual predicate before Arrow conversion.
                if let Some(r) = &self.residual {
                    if let Err(error) = r.apply_with_query(&mut cols, self.query.as_ref()) {
                        self.cursor = None;
                        return Some(Err(error));
                    }
                }
                if let Err(error) = checkpoint(self.query.as_ref()) {
                    self.cursor = None;
                    return Some(Err(error));
                }
                Some(build_cursor_batch(
                    cols,
                    &self.types,
                    &self.schema,
                    self.query.as_ref(),
                ))
            }
            Ok(None) => {
                self.cursor = None;
                None
            }
            Err(e) => {
                self.cursor = None;
                Some(Err(DataFusionError::External(Box::new(
                    MongrelQueryError::Core(e),
                ))))
            }
        }
    }
}

/// Materialize one `RecordBatch` for the row range `[start, end)` by slicing
/// every shared column and converting via [`native_to_array_owned`] (moving
/// typed buffers into Arrow — zero-copy on Int64/Float64).
fn build_chunk_batch(
    columns: &[NativeColumn],
    types: &[TypeId],
    schema: &SchemaRef,
    start: usize,
    end: usize,
    query: Option<&RegisteredSqlQuery>,
) -> datafusion::common::Result<RecordBatch> {
    let mut arrays = Vec::with_capacity(columns.len());
    for (col, ty) in columns.iter().zip(types.iter()) {
        let slice = col.slice_range(start, end);
        checkpoint(query)?;
        arrays.push(native_to_array_owned_with_query(ty.clone(), slice, query).map_err(df_err)?);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| df_err(MongrelQueryError::Arrow(e.to_string())))
}

/// Build a `RecordBatch` from whole (already survivor-gathered) native columns
/// returned by the cursor, in projection order. Consumes the columns so typed
/// buffers move into Arrow without a copy.
fn build_cursor_batch(
    cols: Vec<NativeColumn>,
    types: &[TypeId],
    schema: &SchemaRef,
    query: Option<&RegisteredSqlQuery>,
) -> datafusion::common::Result<RecordBatch> {
    let mut arrays = Vec::with_capacity(cols.len());
    for (col, ty) in cols.into_iter().zip(types.iter()) {
        checkpoint(query)?;
        arrays.push(native_to_array_owned_with_query(ty.clone(), col, query).map_err(df_err)?);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| df_err(MongrelQueryError::Arrow(e.to_string())))
}

/// Build a zero-column `RecordBatch` whose only data is its `n`-row count
/// (the `COUNT(*)` path). An empty-schema batch cannot infer its row count
/// from a column, so it must be set explicitly via options.
fn build_row_count_batch(schema: &SchemaRef, n: usize) -> datafusion::common::Result<RecordBatch> {
    let opts = RecordBatchOptions::new().with_row_count(Some(n));
    RecordBatch::try_new_with_options(schema.clone(), vec![], &opts)
        .map_err(|e| df_err(MongrelQueryError::Arrow(e.to_string())))
}

fn df_err(e: MongrelQueryError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

fn checkpoint(query: Option<&RegisteredSqlQuery>) -> datafusion::common::Result<()> {
    query
        .map(RegisteredSqlQuery::checkpoint)
        .transpose()
        .map(|_| ())
        .map_err(df_err)
}

#[cfg(test)]
mod execution_control_tests {
    use super::*;
    use crate::query_registry::{SqlQueryOptions, SqlQueryRegistry};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    fn task_context(query: RegisteredSqlQuery) -> Arc<TaskContext> {
        let context = TaskContext::default();
        let config = context
            .session_config()
            .clone()
            .with_extension(Arc::new(SqlTaskContext::new(query)));
        Arc::new(context.with_session_config(config))
    }

    #[tokio::test]
    async fn reused_physical_plan_gets_fresh_execution_control() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let plan = Arc::new(MongrelScanExec::new(
            schema,
            vec![NativeColumn::Int64 {
                data: vec![1, 2, 3],
                validity: vec![0b111],
            }],
            vec![TypeId::Int64],
            3,
            vec![ColumnStatistics::new_unknown()],
        ));
        let registry = Arc::new(SqlQueryRegistry::default());
        let first = registry.register(SqlQueryOptions::default()).unwrap();
        let second = registry.register(SqlQueryOptions::default()).unwrap();

        assert_ne!(first.id(), second.id());
        assert_eq!(
            first.request_cancel(mongreldb_core::CancellationReason::ClientRequest),
            crate::CancelOutcome::Accepted
        );
        assert!(plan.execute(0, task_context(first)).is_err());

        let mut stream = plan.execute(0, task_context(second)).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 3);
        assert!(stream.next().await.is_none());
    }
}
