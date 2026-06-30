//! DataFusion SQL + Arrow frontend for MongrelDB.
//!
//! [`MongrelProvider`] implements DataFusion's `TableProvider`: each `scan()`
//! takes an MVCC snapshot of the table, materializes the visible columns, and
//! hands DataFusion a streaming `MongrelScanExec` (see `scan.rs`) that emits one
//! `RecordBatch` per 65 536-row chunk. DataFusion then runs the SQL —
//! projection, filter, aggregation, limit — with its own vectorized kernels,
//! pipelined across those small batches so a `LIMIT` short-circuits and peak
//! memory stays bounded. MongrelDB owns storage/writes/indexes; DataFusion owns
//! the vectorized execution.
//!
//! Example (skipped from doctests; see `tests/sql.rs` for runnable ones):
//! ```ignore
//! # use mongreldb_core::Table;
//! # use mongreldb_query::MongrelSession;
//! # async fn run() -> anyhow::Result<()> {
//! let db = Table::create("travel.mongreldb", /* schema */ unimplemented!(), 1)?;
//! let session = MongrelSession::new(db);
//! session.register("travel_trips").await?;
//! let batches = session.run("select * from travel_trips where cost < 300").await?;
//! # Ok(()) }
//! ```

pub mod arrow_conv;
mod commands;
mod error;
mod fk_join;
mod native_agg;
mod scan;
mod shadow;
mod udf;

pub use error::{MongrelQueryError, Result};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use mongreldb_core::{AlterColumn, ColumnFlags, Cursor, Database, Table};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A MongrelDB table exposed to DataFusion. Holds the live `Table` behind a mutex;
/// each scan takes a fresh MVCC snapshot.
pub struct MongrelProvider {
    db: Arc<Mutex<Table>>,
    schema: SchemaRef,
}

impl MongrelProvider {
    pub fn new(db: Arc<Mutex<Table>>) -> Result<Self> {
        let schema = {
            let db = db.lock();
            arrow_conv::arrow_schema(db.schema())?
        };
        Ok(Self { db, schema })
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Debug for MongrelProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongrelProvider").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TableProvider for MongrelProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Tell DataFusion which filters the pushdown serves exactly so it does not
    /// double-filter (and, for `ann_search`, never evaluates the no-op UDF).
    /// LIKE/FM is `Inexact`: the FM pushdown is a substring *superset*, so
    /// DataFusion must still re-apply the real wildcard semantics.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        use datafusion::logical_expr::TableProviderFilterPushDown;
        let schema_ref = self.db.lock().schema().clone();
        Ok(filters
            .iter()
            .map(|f| match translate_filter(f, &schema_ref) {
                Some(
                    mongreldb_core::Condition::FmContains { .. }
                    | mongreldb_core::Condition::FmContainsAll { .. },
                ) => TableProviderFilterPushDown::Inexact,
                Some(_) => TableProviderFilterPushDown::Exact,
                None => match translate_ann_search(f, &schema_ref)
                    .or_else(|| translate_sparse_match(f, &schema_ref))
                {
                    Some(_) => TableProviderFilterPushDown::Exact,
                    None => TableProviderFilterPushDown::Unsupported,
                },
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let core_err = |e: mongreldb_core::MongrelError| {
            DataFusionError::External(Box::new(MongrelQueryError::Core(e)))
        };
        let mut db = self.db.lock();
        let snap = db.snapshot();
        let schema_ref = db.schema().clone();

        // Translate WHERE filters into index-backed Conditions.
        let translated: Vec<mongreldb_core::Condition> = filters
            .iter()
            .filter_map(|f| {
                translate_filter(f, &schema_ref)
                    .or_else(|| translate_ann_search(f, &schema_ref))
                    .or_else(|| translate_sparse_match(f, &schema_ref))
            })
            .collect();

        // `COUNT(*)`-style queries (empty projection) need only a row count.
        // Unfiltered ⇒ O(1) via the maintained `live_count` metadata; a pushed
        // WHERE ⇒ decode one column through the pushdown path to count survivors.
        let empty_proj = projection.map(|p| p.is_empty()).unwrap_or(false);
        if empty_proj {
            let total: usize = if translated.is_empty() {
                mongreldb_core::trace::QueryTrace::record(|t| {
                    t.scan_mode = mongreldb_core::trace::ScanMode::CountMetadata;
                });
                db.count() as usize
            } else if let Some(count) = db.count_conditions(&translated, snap).map_err(core_err)? {
                count as usize
            } else {
                match schema_ref.columns.first() {
                    Some(cdef) => {
                        let one = [cdef.id];
                        let cols = match db
                            .query_columns_native_cached(&translated, Some(&one), snap)
                            .map_err(core_err)?
                        {
                            Some(c) => c,
                            None => db
                                .visible_columns_native(snap, Some(&one))
                                .map_err(core_err)?,
                        };
                        mongreldb_core::trace::QueryTrace::record(|t| {
                            t.scan_mode = mongreldb_core::trace::ScanMode::Materialized;
                        });
                        cols.first().map(|(_, c)| c.len()).unwrap_or(0)
                    }
                    None => 0,
                }
            };
            return Ok(Arc::new(scan::MongrelScanExec::new_row_count(total)));
        }

        // Output column ids + Arrow schema for this scan, in scan-field order.
        // DataFusion's projection already includes every column a retained
        // (Inexact / Unsupported) filter still needs, so decoding exactly this
        // set is correct. `None` ⇒ the full schema.
        let (col_ids, scan_schema): (Vec<u16>, SchemaRef) = match projection {
            Some(p) if !p.is_empty() => {
                let ids = p.iter().map(|&idx| schema_ref.columns[idx].id).collect();
                let fields: Vec<arrow::datatypes::Field> = p
                    .iter()
                    .map(|&idx| self.schema.field(idx).clone())
                    .collect();
                (ids, Arc::new(arrow::datatypes::Schema::new(fields)))
            }
            _ => (
                schema_ref.columns.iter().map(|c| c.id).collect(),
                self.schema.clone(),
            ),
        };

        // Projection pairs (column id, type) in scan-field order.
        let mut proj_pairs: Vec<(u16, mongreldb_core::schema::TypeId)> =
            Vec::with_capacity(col_ids.len());
        let mut types: Vec<mongreldb_core::schema::TypeId> = Vec::with_capacity(col_ids.len());
        for cid in &col_ids {
            let ty = schema_ref
                .columns
                .iter()
                .find(|c| c.id == *cid)
                .map(|c| c.ty)
                .ok_or_else(|| {
                    DataFusionError::External(Box::new(MongrelQueryError::Arrow(format!(
                        "unknown column {cid}"
                    ))))
                })?;
            proj_pairs.push((*cid, ty));
            types.push(ty);
        }

        // Phase 7.1: exact per-column min/max from page stats, but only for an
        // unfiltered full scan over an insert-only table (gated in core). A
        // pushed WHERE or a table with deletes ⇒ all-Absent (DataFusion scans).
        let col_stats_map = if translated.is_empty() {
            db.exact_column_stats(snap, &col_ids).map_err(core_err)?
        } else {
            None
        };
        let column_stats: Vec<datafusion::physical_plan::ColumnStatistics> = col_ids
            .iter()
            .map(|cid| scan::to_col_statistics(col_stats_map.as_ref().and_then(|m| m.get(cid))))
            .collect();

        // Phase 15.5: Arrow IPC shadow — zero-copy scan for clean single-run
        // unfiltered tables. The shadow is a derived Arrow IPC file that was
        // written on a prior scan; reading it avoids per-column decode entirely.
        if translated.is_empty()
            && db.run_count() == 1
            && db.memtable_is_empty()
            && db.mutable_run_len() == 0
            && db.single_run_is_clean()
        {
            let shadow = shadow::ArrowShadow::new(db.dir());
            let run_ids: HashSet<u128> = db.run_ids().into_iter().collect();
            shadow.sweep(&run_ids);
            if let Some(&run_id) = run_ids.iter().next() {
                if let Some(batch) = shadow.try_read(run_id) {
                    if let Some(projected) =
                        project_batch(&batch, &col_ids, &schema_ref, &scan_schema)
                    {
                        mongreldb_core::trace::QueryTrace::record(|t| {
                            t.scan_mode = mongreldb_core::trace::ScanMode::ArrowShadow;
                        });
                        return Ok(Arc::new(scan::MongrelScanExec::new_batch(
                            scan_schema,
                            projected,
                            column_stats,
                        )));
                    }
                }
            }
        }

        // Phase 6.2 / 16.1: drive a lazy streaming cursor that fuses the
        // predicate, skips pages with no survivors, and decodes only the
        // projected columns of surviving pages. `scan_cursor` picks the page-plan
        // fast path for a single run or the k-way-merge cursor for multi-run —
        // both avoid fully materializing every row. Anything else (e.g. an empty
        // table with only memtable rows) falls through to materialize-then-chunk.
        let cursor: Option<Box<dyn Cursor>> = db
            .scan_cursor(snap, proj_pairs, &translated)
            .map_err(core_err)?;
        if let Some(cursor) = cursor {
            let num_rows = cursor.remaining_rows();
            // Phase 16.3a: extract the LIKE pattern for residual pre-filtering.
            let residual = extract_residual_filter(filters, &col_ids, &schema_ref);
            return Ok(Arc::new(scan::MongrelScanExec::new_cursor(
                scan_schema,
                types,
                cursor,
                num_rows,
                column_stats,
                residual,
            )));
        }

        // Pushdown returns exactly `col_ids` when it accepts; the full-scan
        // fallback returns all columns, of which we keep `col_ids`.
        let cols = if !translated.is_empty() {
            match db
                .query_columns_native_cached(&translated, Some(&col_ids), snap)
                .map_err(core_err)?
            {
                Some(c) => c,
                None => db
                    .visible_columns_native(snap, Some(&col_ids))
                    .map_err(core_err)?,
            }
        } else {
            db.visible_columns_native(snap, Some(&col_ids))
                .map_err(core_err)?
        };

        // Order the decoded columns into scan-field order for the streaming exec.
        let mut ordered: Vec<mongreldb_core::columnar::NativeColumn> =
            Vec::with_capacity(col_ids.len());
        for cid in &col_ids {
            let col = cols
                .iter()
                .find(|(id, _)| id == cid)
                .map(|(_, c)| c.clone())
                .ok_or_else(|| {
                    DataFusionError::External(Box::new(MongrelQueryError::Arrow(format!(
                        "missing column {cid}"
                    ))))
                })?;
            ordered.push(col);
        }
        let num_rows = ordered.first().map(|c| c.len()).unwrap_or(0);

        // Collect data needed for the shadow write before releasing the lock.
        let shadow_write: Option<(
            std::path::PathBuf,
            u128,
            Vec<arrow::array::ArrayRef>,
            SchemaRef,
        )> = if translated.is_empty()
            && db.run_count() == 1
            && db.memtable_is_empty()
            && db.mutable_run_len() == 0
            && db.single_run_is_clean()
        {
            let all_schema_ids: Vec<u16> = schema_ref.columns.iter().map(|c| c.id).collect();
            if col_ids == all_schema_ids {
                let dir = db.dir().to_path_buf();
                let run_id = db.run_ids().first().copied();
                run_id.map(|rid| {
                    let arrays = ordered
                        .iter()
                        .zip(types.iter())
                        .map(|(col, &ty)| arrow_conv::native_to_array(ty, col))
                        .collect::<Result<_>>()
                        .unwrap_or_default();
                    (dir, rid, arrays, scan_schema.clone())
                })
            } else {
                None
            }
        } else {
            None
        };

        drop(db);

        // Phase 15.5: write the Arrow IPC shadow for future scans (best-effort,
        // outside the Table lock).
        if let Some((dir, run_id, arrays, schema)) = shadow_write {
            if let Ok(batch) = RecordBatch::try_new(schema, arrays) {
                shadow::ArrowShadow::new(&dir).write(run_id, &batch);
            }
        }

        mongreldb_core::trace::QueryTrace::record(|t| {
            t.scan_mode = mongreldb_core::trace::ScanMode::Materialized;
            t.row_materialized = true;
        });
        Ok(Arc::new(scan::MongrelScanExec::new(
            scan_schema,
            ordered,
            types,
            num_rows,
            column_stats,
        )))
    }
}

/// Phase 15.5: project columns from a full-schema shadow `RecordBatch` to match
/// the scan's requested column IDs and Arrow schema. Returns `None` if any
/// requested column is not present in the shadow batch (schema mismatch → miss).
fn project_batch(
    batch: &RecordBatch,
    col_ids: &[u16],
    schema_ref: &mongreldb_core::schema::Schema,
    scan_schema: &arrow::datatypes::SchemaRef,
) -> Option<RecordBatch> {
    // Map schema column ids to field names for lookup in the shadow batch.
    let arrays: Vec<arrow::array::ArrayRef> = col_ids
        .iter()
        .map(|cid| {
            // Find the column name for this id in the live schema.
            let name = schema_ref
                .columns
                .iter()
                .find(|c| c.id == *cid)
                .map(|c| c.name.as_str())?;
            // Look up the column in the shadow batch by name.
            let idx = batch.schema().index_of(name).ok()?;
            Some(batch.column(idx).clone())
        })
        .collect::<Option<Vec<_>>>()?;
    RecordBatch::try_new(scan_schema.clone(), arrays).ok()
}

/// Translate a DataFusion WHERE filter expression into a MongrelDB
/// index-backed [`Condition`]. Supported translations (all index/scan-served by
/// `Table::query_columns_native`):
///
/// * `col = literal` → [`Condition::BitmapEq`] (bitmap index) or
///   [`Condition::Pk`] (primary key).
/// * `col <, >, <=, >= literal` and `col BETWEEN a AND b` →
///   [`Condition::Range`] (Int64) / [`Condition::RangeF64`] (Float64).
/// * `col LIKE '%pat%'` → [`Condition::FmContains`] (FM index). Any `%`/`_`
///   wildcard pattern is mapped to its longest literal segment; DataFusion
///   re-applies the real LIKE on the returned batch, so correctness is exact
///   even though the pushdown is a substring superset.
///
/// Everything else is left to DataFusion's post-scan filter. Because DataFusion
/// always re-applies the full WHERE on the returned batch, a pushdown only ever
/// needs to return a *superset* of the survivors — it is a pure optimization,
/// never a correctness risk.
pub(crate) fn translate_filter(
    expr: &Expr,
    schema: &mongreldb_core::Schema,
) -> Option<mongreldb_core::Condition> {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Between, BinaryExpr, Like, Operator};
    use mongreldb_core::{ColumnFlags, Condition, IndexKind, TypeId, Value};

    // Extended int extraction: handles Date32 and all Timestamp* precision
    // variants that DataFusion emits for typed column comparisons. These are
    // stored as Int64 internally, so the numeric value is the raw i64.
    let int_val = |s: &ScalarValue| match s {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Date32(Some(v)) => Some(*v as i64),
        ScalarValue::TimestampSecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        _ => None,
    };
    let float_val = |s: &ScalarValue| match s {
        ScalarValue::Float64(Some(f)) => Some(*f),
        _ => None,
    };
    let bytes_val = |s: &ScalarValue| match s {
        ScalarValue::Utf8(Some(s)) => Some(s.as_bytes().to_vec()),
        _ => None,
    };
    let _ = bytes_val; // retained for clarity; equality uses the generic `val` below.

    let val = |s: &ScalarValue| -> Option<Value> {
        match s {
            ScalarValue::Int64(Some(v)) => Some(Value::Int64(*v)),
            ScalarValue::Utf8(Some(s)) => Some(Value::Bytes(s.as_bytes().to_vec())),
            ScalarValue::Float64(Some(f)) => Some(Value::Float64(*f)),
            ScalarValue::Boolean(Some(b)) => Some(Value::Bool(*b)),
            _ => None,
        }
    };

    let col_def = |name: &str| schema.columns.iter().find(|c| c.name == name);
    let has_fm = |cid: u16| {
        schema
            .indexes
            .iter()
            .any(|i| i.column_id == cid && i.kind == IndexKind::FmIndex)
    };
    let has_bitmap = |cid: u16| {
        schema
            .indexes
            .iter()
            .any(|i| i.column_id == cid && i.kind == IndexKind::Bitmap)
    };

    match expr {
        // `col OP literal` (and the mirrored `literal OP col`).
        // Also handles `col = v1 OR col = v2 OR ...` → BitmapIn.
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // OR-of-equalities on the same column → BitmapIn (Priority 6).
            if *op == Operator::Or {
                return try_or_as_bitmap_in(expr, schema);
            }
            // Unwrap single-layer Cast wrappers (canonicalization).
            let left = peel_cast(left);
            let right = peel_cast(right);
            let (col_name, scalar, flipped) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(s, _)) => (&c.name, s, false),
                (Expr::Literal(s, _), Expr::Column(c)) => (&c.name, s, true),
                _ => return None,
            };
            let op = if flipped { flip_op(*op)? } else { *op };
            let cdef = col_def(col_name)?;

            // Equality: bitmap index or primary key.
            if op == Operator::Eq {
                let v = val(scalar)?;
                if has_bitmap(cdef.id) {
                    return Some(Condition::BitmapEq {
                        column_id: cdef.id,
                        value: v.encode_key(),
                    });
                }
                if cdef.flags.contains(ColumnFlags::PRIMARY_KEY) {
                    return Some(Condition::Pk(v.encode_key()));
                }
                return None;
            }

            // Range on a typed numeric column.
            match cdef.ty {
                TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                    let v = int_val(scalar)?;
                    let (lo, hi) = int_bounds(op, v)?;
                    Some(Condition::Range {
                        column_id: cdef.id,
                        lo,
                        hi,
                    })
                }
                TypeId::Float64 => {
                    let v = float_val(scalar)?;
                    let (lo, lo_inc, hi, hi_inc) = float_bounds(op, v)?;
                    Some(Condition::RangeF64 {
                        column_id: cdef.id,
                        lo,
                        lo_inclusive: lo_inc,
                        hi,
                        hi_inclusive: hi_inc,
                    })
                }
                _ => None,
            }
        }

        // `col BETWEEN low AND high` (and `col NOT BETWEEN ...` → skip).
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if *negated {
                return None;
            }
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let cdef = col_def(&c.name)?;
            let (lo_s, hi_s) = match (low.as_ref(), high.as_ref()) {
                (Expr::Literal(lo, _), Expr::Literal(hi, _)) => (lo, hi),
                _ => return None,
            };
            match cdef.ty {
                TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                    let (Some(lo), Some(hi)) = (int_val(lo_s), int_val(hi_s)) else {
                        return None;
                    };
                    Some(Condition::Range {
                        column_id: cdef.id,
                        lo,
                        hi,
                    })
                }
                TypeId::Float64 => {
                    let (Some(lo), Some(hi)) = (float_val(lo_s), float_val(hi_s)) else {
                        return None;
                    };
                    Some(Condition::RangeF64 {
                        column_id: cdef.id,
                        lo,
                        lo_inclusive: true,
                        hi,
                        hi_inclusive: true,
                    })
                }
                _ => None,
            }
        }

        // `col LIKE pattern` → FM-index substring on the longest literal segment.
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            ..
        }) => {
            if *negated {
                return None;
            }
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let Expr::Literal(ScalarValue::Utf8(Some(pat)), _) = pattern.as_ref() else {
                return None;
            };
            let cdef = col_def(&c.name)?;
            if !has_fm(cdef.id) {
                return None;
            }
            // Priority 12: extract ALL literal segments (≥3 chars) and intersect
            // their FM results for a much tighter superset than the single
            // longest segment. Falls back to the longest when only one qualifies.
            let segments: Vec<Vec<u8>> = pat
                .split(['%', '_'])
                .filter(|s| s.len() >= 3)
                .map(|s| s.as_bytes().to_vec())
                .collect();
            match segments.len() {
                0 => longest_like_segment(pat).map(|seg| Condition::FmContains {
                    column_id: cdef.id,
                    pattern: seg,
                }),
                1 => Some(Condition::FmContains {
                    column_id: cdef.id,
                    pattern: segments.into_iter().next().unwrap(),
                }),
                _ => Some(Condition::FmContainsAll {
                    column_id: cdef.id,
                    patterns: segments,
                }),
            }
        }

        // `col IN (lit1, lit2, …)` → BitmapIn (bitmap union). Phase 13.5:
        // runtime-filter pushdown for semi-joins and IN-list filters. Only when
        // the column has a bitmap index and every list entry is a literal.
        Expr::InList(il) if !il.negated => {
            let Expr::Column(c) = il.expr.as_ref() else {
                return None;
            };
            let cdef = col_def(&c.name)?;
            if !has_bitmap(cdef.id) {
                return None;
            }
            let values: Vec<Vec<u8>> = il
                .list
                .iter()
                .filter_map(|e| match e {
                    Expr::Literal(s, _) => val(s).map(|v| v.encode_key()),
                    _ => None,
                })
                .collect();
            if values.is_empty() || values.len() != il.list.len() {
                return None;
            }
            Some(Condition::BitmapIn {
                column_id: cdef.id,
                values,
            })
        }

        // `col IS NULL` → page-stat-pruned column scan for null validity.
        Expr::IsNull(inner) => {
            let col_name = match inner.as_ref() {
                Expr::Column(c) => &c.name,
                _ => return None,
            };
            let cdef = col_def(col_name)?;
            Some(Condition::IsNull { column_id: cdef.id })
        }

        // `col IS NOT NULL` → complement of IS NULL.
        Expr::IsNotNull(inner) => {
            let col_name = match inner.as_ref() {
                Expr::Column(c) => &c.name,
                _ => return None,
            };
            let cdef = col_def(col_name)?;
            Some(Condition::IsNotNull { column_id: cdef.id })
        }

        _ => None,
    }
}

/// Phase 16.3a: extract the SQL `LIKE` pattern from `filters` for residual
/// pre-filtering on `NativeColumn` buffers. Returns a `ResidualFilter` when a
/// non-negated LIKE on a Bytes column is found among the filters.
pub(crate) fn extract_residual_filter(
    filters: &[Expr],
    col_ids: &[u16],
    schema: &mongreldb_core::Schema,
) -> Option<std::sync::Arc<scan::ResidualFilter>> {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Like;
    for f in filters {
        if let Expr::Like(Like {
            negated: false,
            expr,
            pattern,
            ..
        }) = f
        {
            let Expr::Column(c) = expr.as_ref() else {
                continue;
            };
            let Expr::Literal(ScalarValue::Utf8(Some(pat)), _) = pattern.as_ref() else {
                continue;
            };
            let cdef = schema.columns.iter().find(|col| col.name == c.name)?;
            let col_idx = col_ids.iter().position(|&id| id == cdef.id)?;
            return Some(std::sync::Arc::new(scan::ResidualFilter::new(
                col_idx,
                pat.as_bytes().to_vec(),
            )));
        }
    }
    None
}

/// Translate `ann_search(<embedding-col>, '<json f32 array>', k)` — the SQL hook
/// for HNSW semantic search — into [`Condition::Ann`]. The `ann_search` UDF is
/// registered by [`MongrelSession`] purely so the SQL parses; the provider's
/// pushdown serves the real top-k, and `supports_filters_pushdown` marks the
/// filter `Exact` so DataFusion never evaluates the (no-op) UDF itself.
pub(crate) fn translate_ann_search(
    expr: &Expr,
    schema: &mongreldb_core::Schema,
) -> Option<mongreldb_core::Condition> {
    use datafusion::common::ScalarValue;
    use mongreldb_core::Condition;

    let Expr::ScalarFunction(sf) = expr else {
        return None;
    };
    if !sf.func.name().eq_ignore_ascii_case("ann_search") || sf.args.len() != 3 {
        return None;
    }
    let (Expr::Column(c), query_expr, k_expr) = (&sf.args[0], &sf.args[1], &sf.args[2]) else {
        return None;
    };
    let cdef = schema.columns.iter().find(|col| col.name == c.name)?;
    let json = match query_expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.as_str(),
        _ => return None,
    };
    let k: i64 = match k_expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int64(Some(k)) => *k,
            ScalarValue::UInt64(Some(k)) => *k as i64,
            ScalarValue::Int32(Some(k)) => *k as i64,
            _ => return None,
        },
        _ => return None,
    };
    let query: Vec<f32> = serde_json::from_str(json).ok()?;
    Some(Condition::Ann {
        column_id: cdef.id,
        query,
        k: k.max(1) as usize,
    })
}

/// Translate `sparse_match(<sparse-col>, '<json [[token, weight], …]>', k)` —
/// the SQL hook for SPLADE-style sparse retrieval — into
/// [`Condition::SparseMatch`]. The UDF is registered by [`MongrelSession`]
/// purely so the SQL parses; the provider's pushdown serves the real top-k.
pub(crate) fn translate_sparse_match(
    expr: &Expr,
    schema: &mongreldb_core::Schema,
) -> Option<mongreldb_core::Condition> {
    use datafusion::common::ScalarValue;
    use mongreldb_core::Condition;

    let Expr::ScalarFunction(sf) = expr else {
        return None;
    };
    if !sf.func.name().eq_ignore_ascii_case("sparse_match") || sf.args.len() != 3 {
        return None;
    }
    let (Expr::Column(c), query_expr, k_expr) = (&sf.args[0], &sf.args[1], &sf.args[2]) else {
        return None;
    };
    let cdef = schema.columns.iter().find(|col| col.name == c.name)?;
    let json = match query_expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.as_str(),
        _ => return None,
    };
    let k: i64 = match k_expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int64(Some(k)) => *k,
            ScalarValue::UInt64(Some(k)) => *k as i64,
            ScalarValue::Int32(Some(k)) => *k as i64,
            _ => return None,
        },
        _ => return None,
    };
    let query: Vec<(u32, f32)> = serde_json::from_str(json).ok()?;
    Some(Condition::SparseMatch {
        column_id: cdef.id,
        query,
        k: k.max(1) as usize,
    })
}

/// Mirror a comparison operator for the `literal OP col` form.
fn flip_op(op: datafusion::logical_expr::Operator) -> Option<datafusion::logical_expr::Operator> {
    use datafusion::logical_expr::Operator;
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::Lt => Operator::Gt,
        Operator::Gt => Operator::Lt,
        Operator::LtEq => Operator::GtEq,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

/// Convert `col OP v` into inclusive Int64 `[lo, hi]` bounds (exact for all of
/// `<`, `>`, `<=`, `>=` via saturating ±1).
fn int_bounds(op: datafusion::logical_expr::Operator, v: i64) -> Option<(i64, i64)> {
    use datafusion::logical_expr::Operator;
    Some(match op {
        Operator::Gt => (v.saturating_add(1), i64::MAX),
        Operator::GtEq => (v, i64::MAX),
        Operator::Lt => (i64::MIN, v.saturating_sub(1)),
        Operator::LtEq => (i64::MIN, v),
        _ => return None,
    })
}

/// Convert `col OP v` into Float64 bounds with per-bound inclusivity.
fn float_bounds(op: datafusion::logical_expr::Operator, v: f64) -> Option<(f64, bool, f64, bool)> {
    use datafusion::logical_expr::Operator;
    Some(match op {
        Operator::Gt => (v, false, f64::INFINITY, false),
        Operator::GtEq => (v, true, f64::INFINITY, false),
        Operator::Lt => (f64::NEG_INFINITY, false, v, false),
        Operator::LtEq => (f64::NEG_INFINITY, false, v, true),
        _ => return None,
    })
}

/// Longest contiguous literal (non-`%`, non-`_`) segment of a SQL LIKE pattern;
/// `None` if the pattern is all wildcards (matches everything → no pushdown).
/// Splitting on BOTH wildcards (not just `%`) keeps the segment a true literal
/// substring of every match, so the FM-index search is a correct *superset* —
/// e.g. `%City_1%` ⇒ segment `City` (not the literal `City_1`, which no match
/// like `City11` actually contains). DataFusion re-applies the real wildcard.
fn longest_like_segment(pat: &str) -> Option<Vec<u8>> {
    pat.split(['%', '_'])
        .map(|s| s.as_bytes())
        .max_by_key(|s| s.len())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
}

/// Unwrap a single-layer `Expr::Cast` wrapper to enable pushdown for queries
/// like `WHERE CAST(col AS BIGINT) = 5` (canonicalization). Returns the
/// original `Box` unchanged for non-cast expressions.
fn peel_cast(expr: &Expr) -> std::borrow::Cow<'_, Expr> {
    match expr {
        Expr::Cast(datafusion::logical_expr::Cast { expr, .. }) => std::borrow::Cow::Borrowed(expr),
        _ => std::borrow::Cow::Borrowed(expr),
    }
}

/// Flatten an OR tree of same-column equality comparisons into a `BitmapIn`.
/// Handles `col = v1 OR col = v2 OR ...` (and nested OR) that DataFusion's
/// optimizer may not have rewritten into `IN`. Returns `None` if the OR spans
/// different columns, non-equality comparisons, or a non-bitmap-indexed column.
fn try_or_as_bitmap_in(
    expr: &Expr,
    schema: &mongreldb_core::Schema,
) -> Option<mongreldb_core::Condition> {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut target_col: Option<u16> = None;
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => {
                stack.push(left);
                stack.push(right);
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => {
                let (col_name, scalar) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(s, _)) => (&c.name, s),
                    (Expr::Literal(s, _), Expr::Column(c)) => (&c.name, s),
                    _ => return None,
                };
                let cdef = schema.columns.iter().find(|c| &c.name == col_name)?;
                if !schema
                    .indexes
                    .iter()
                    .any(|i| i.column_id == cdef.id && i.kind == mongreldb_core::IndexKind::Bitmap)
                {
                    return None;
                }
                match target_col {
                    None => target_col = Some(cdef.id),
                    Some(id) if id != cdef.id => return None,
                    _ => {}
                }
                let v = match scalar {
                    datafusion::common::ScalarValue::Int64(Some(v)) => {
                        mongreldb_core::Value::Int64(*v)
                    }
                    datafusion::common::ScalarValue::Utf8(Some(s)) => {
                        mongreldb_core::Value::Bytes(s.as_bytes().to_vec())
                    }
                    datafusion::common::ScalarValue::Float64(Some(f)) => {
                        mongreldb_core::Value::Float64(*f)
                    }
                    datafusion::common::ScalarValue::Boolean(Some(b)) => {
                        mongreldb_core::Value::Bool(*b)
                    }
                    _ => return None,
                };
                values.push(v.encode_key());
            }
            _ => return None,
        }
    }
    let col_id = target_col?;
    if values.is_empty() {
        return None;
    }
    Some(mongreldb_core::Condition::BitmapIn {
        column_id: col_id,
        values,
    })
}

/// Convenience wrapper: a DataFusion `SessionContext` bound to a live MongrelDB,
/// with a result cache keyed by `(sql, snapshot_epoch)` that auto-invalidates
/// when a commit advances the epoch.
pub struct MongrelSession {
    ctx: SessionContext,
    db: Option<Arc<Mutex<Table>>>,
    /// P4.1: the multi-table `Database` when opened via `open()`. When `Some`,
    /// the cache epoch is driven by `Database::visible_epoch()` instead of the
    /// legacy `combined_epoch()` fold.
    database: Option<Arc<Database>>,
    cache: ResultCache,
    /// Phase 16.5: logical-plan cache keyed by SQL string.
    plan_cache: parking_lot::Mutex<HashMap<String, datafusion::logical_expr::LogicalPlan>>,
    /// `table name → owning Table handle` for every registered table.
    tables: parking_lot::Mutex<HashMap<String, Arc<Mutex<Table>>>>,
    /// Phase 17.3: named materialized views — `view name → defining SQL`.
    /// On `run("SELECT * FROM <view>")`, the defining SQL is executed (or the
    /// result-cache is hit). Invalidated automatically on commit (epoch bump).
    views: parking_lot::Mutex<HashMap<String, String>>,
    /// SQL `BEGIN`/`COMMIT` staging for DML statements. Reads remain
    /// snapshot-at-scan; this batches SQL writes atomically when a client sends
    /// an explicit transaction block.
    sql_txn: parking_lot::Mutex<Option<Vec<commands::PendingSqlOp>>>,
}

/// `(sql, snapshot_epoch) → cached result batches`.
type CacheKey = (String, u64);
type ResultCache = parking_lot::Mutex<std::collections::HashMap<CacheKey, Arc<Vec<RecordBatch>>>>;

impl MongrelSession {
    /// Create a session over a live `Table`. Takes ownership; wrap in `Arc` if you
    /// need to keep a handle for writes after registering the provider. Registers
    /// the `ann_search` UDF so SQL semantic-search predicates parse.
    pub fn new(db: Table) -> Self {
        let db = Arc::new(Mutex::new(db));
        let ctx = SessionContext::new();
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
            udf::AnnSearchUdf::new(),
        ));
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
            udf::SparseMatchUdf::new(),
        ));
        Self {
            ctx,
            db: Some(db),
            database: None,
            cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            plan_cache: parking_lot::Mutex::new(HashMap::new()),
            tables: parking_lot::Mutex::new(HashMap::new()),
            views: parking_lot::Mutex::new(HashMap::new()),
            sql_txn: parking_lot::Mutex::new(None),
        }
    }

    /// Open a session over a multi-table [`Database`] (spec §12). Auto-registers
    /// every live table as a `MongrelProvider`; the cache epoch is driven by
    /// `Database::visible_epoch()` so any table's commit invalidates cached
    /// results.
    pub fn open(database: Arc<Database>) -> Result<Self> {
        let ctx = SessionContext::new();
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
            udf::AnnSearchUdf::new(),
        ));
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(
            udf::SparseMatchUdf::new(),
        ));

        let mut tables: HashMap<String, Arc<Mutex<Table>>> = HashMap::new();
        for name in database.table_names() {
            let handle = database.table(&name)?;
            let provider = MongrelProvider::new(handle.clone())?;
            ctx.register_table(&name, Arc::new(provider))
                .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
            tables.insert(name, handle);
        }

        // Pick a stable "primary" (lexicographically smallest name) for legacy
        // `db()` accessors. If the database is empty, `db()` returns `None`.
        let primary = {
            let mut names: Vec<&String> = tables.keys().collect();
            names.sort();
            names.first().and_then(|n| tables.get(*n).cloned())
        };

        Ok(Self {
            ctx,
            db: primary,
            database: Some(database),
            cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            plan_cache: parking_lot::Mutex::new(HashMap::new()),
            tables: parking_lot::Mutex::new(tables),
            views: parking_lot::Mutex::new(HashMap::new()),
            sql_txn: parking_lot::Mutex::new(None),
        })
    }

    /// The underlying Table handle (Phase 19.3: used by the daemon for direct
    /// put/delete/commit/count access). Returns `None` when the session was
    /// opened over an empty `Database`.
    pub fn db(&self) -> Option<&Arc<Mutex<Table>>> {
        self.db.as_ref()
    }

    /// Phase 17.3: create a named materialized view backed by a SQL query.
    /// `SELECT * FROM <name>` resolves to the view's defining SQL, which is
    /// executed (or served from the result cache) transparently. The view is
    /// automatically invalidated on commit (via the epoch-keyed result cache).
    pub fn create_view(&self, name: &str, sql: &str) {
        self.views.lock().insert(name.to_string(), sql.to_string());
    }

    /// Drop a named materialized view.
    pub fn drop_view(&self, name: &str) {
        self.views.lock().remove(name);
    }

    /// Register the table under `name` so `select * from <name>` resolves.
    pub async fn register(&self, name: &str) -> Result<()> {
        let db = self.db.clone().ok_or(MongrelQueryError::Core(
            mongreldb_core::MongrelError::NotFound("no primary table".into()),
        ))?;
        let provider = MongrelProvider::new(db.clone())?;
        self.tables.lock().insert(name.to_string(), db);
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        Ok(())
    }

    /// Register a second (or further) live `Table` as another table on the same
    /// session, enabling cross-table SQL joins. The first `Table` (passed to
    /// [`Self::new`]) still owns the result-cache epoch: cached results are
    /// invalidated on its commits, so mutate the primary table last or call
    /// [`Self::clear_cache`] after writing a secondary table.
    pub async fn register_db(&self, name: &str, db: Table) -> Result<()> {
        let db_arc = Arc::new(Mutex::new(db));
        let provider = MongrelProvider::new(db_arc.clone())?;
        self.tables.lock().insert(name.to_string(), db_arc);
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        Ok(())
    }

    fn refresh_registered_table(&self, db: &Arc<Database>, name: &str) -> Result<()> {
        self.ctx
            .deregister_table(name)
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        let handle = db.table(name)?;
        let provider = MongrelProvider::new(handle.clone())?;
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        self.tables.lock().insert(name.to_string(), handle);
        Ok(())
    }

    /// Run a SQL statement and return the result batches. Repeated identical SQL
    /// against the same snapshot returns the cached batches without re-executing.
    /// Run a SQL statement and return the result batches. DDL statements
    /// (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`) are intercepted when a
    /// `Database` is attached and mapped to the catalog. Repeated identical SQL
    /// against the same snapshot returns the cached batches without re-executing.
    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        if let Some(batches) = commands::try_run_command(self, sql)? {
            return Ok(batches);
        }

        // P4.2: intercept DDL when a Database is attached.
        let lower = sql.trim_start().to_lowercase();
        if lower.starts_with("create table") {
            if let Some(db) = &self.database {
                let (name, schema) = parse_create_table(sql)?;
                db.create_table(&name, schema)?;
                let handle = db.table(&name)?;
                let provider = MongrelProvider::new(handle.clone())?;
                self.ctx
                    .register_table(&name, Arc::new(provider))
                    .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                self.tables.lock().insert(name, handle);
                self.clear_cache();
                return Ok(Vec::new());
            }
        }
        if lower.starts_with("drop table") {
            if let Some(db) = &self.database {
                let (name, if_exists) = parse_drop_table(sql)?;
                let drop_result = db.drop_table(&name);
                if let Err(e) = drop_result {
                    // IF EXISTS tolerates NotFound.
                    let is_not_found = matches!(e, mongreldb_core::MongrelError::NotFound(_));
                    if !(if_exists && is_not_found) {
                        return Err(e.into());
                    }
                } else {
                    self.ctx
                        .deregister_table(&name)
                        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                    self.tables.lock().remove(&name);
                }
                self.clear_cache();
                return Ok(Vec::new());
            }
        }
        if lower.starts_with("alter table") {
            if let Some(db) = &self.database {
                match parse_alter_table(sql)? {
                    ParsedAlterTable::RenameTable { old_name, new_name } => {
                        db.rename_table(&old_name, &new_name)?;
                        // Re-key DataFusion + the session's handle cache under the new
                        // name. The table_id and underlying table object are unchanged
                        // by a rename, so a fresh handle resolves to the same table.
                        self.ctx
                            .deregister_table(&old_name)
                            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                        self.tables.lock().remove(&old_name);
                        let handle = db.table(&new_name)?;
                        let provider = MongrelProvider::new(handle.clone())?;
                        self.ctx
                            .register_table(&new_name, Arc::new(provider))
                            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                        self.tables.lock().insert(new_name, handle);
                    }
                    ParsedAlterTable::RenameColumn {
                        table_name,
                        column_name,
                        new_name,
                    } => {
                        db.alter_column(&table_name, &column_name, AlterColumn::rename(new_name))?;
                        self.refresh_registered_table(db, &table_name)?;
                    }
                    ParsedAlterTable::AlterColumnType {
                        table_name,
                        column_name,
                        ty,
                    } => {
                        db.alter_column(&table_name, &column_name, AlterColumn::set_type(ty))?;
                        self.refresh_registered_table(db, &table_name)?;
                    }
                    ParsedAlterTable::SetNotNull {
                        table_name,
                        column_name,
                    } => {
                        let flags = current_column_flags(db, &table_name, &column_name)?
                            .without(ColumnFlags::NULLABLE);
                        db.alter_column(&table_name, &column_name, AlterColumn::set_flags(flags))?;
                        self.refresh_registered_table(db, &table_name)?;
                    }
                    ParsedAlterTable::DropNotNull {
                        table_name,
                        column_name,
                    } => {
                        let flags = current_column_flags(db, &table_name, &column_name)?
                            .with(ColumnFlags::NULLABLE);
                        db.alter_column(&table_name, &column_name, AlterColumn::set_flags(flags))?;
                        self.refresh_registered_table(db, &table_name)?;
                    }
                }
                self.clear_cache();
                return Ok(Vec::new());
            }
        }

        // Phase 17.3: intercept `SELECT ... FROM <view_name>` and rewrite to
        // the view's defining SQL.
        let resolved = self.resolve_view_sql(sql);
        // Canonicalize whitespace outside literals/comments so queries that
        // differ only in spacing share a cache key (and parse identically — SQL
        // is whitespace-insensitive between tokens).
        let effective_sql = normalize_sql(&resolved);
        let sql = effective_sql.as_str();
        // The cache key uses the Database's visible epoch (P4.1) when opened
        // via `open()`, or the legacy `combined_epoch()` fold for multi-table
        // sessions created via `new()` + `register_db()`.
        let epoch = self.cache_epoch();
        let key = (sql.to_string(), epoch);
        if let Some(hit) = self.cache.lock().get(&key) {
            return Ok((**hit).clone());
        }
        // Phase 16.5: check the logical-plan cache before re-parsing.
        let plan_start = std::time::Instant::now();
        let df = {
            let cached_plan = self.plan_cache.lock().get(sql).cloned();
            if let Some(plan) = cached_plan {
                datafusion::dataframe::DataFrame::new(self.ctx.state(), plan)
            } else {
                let df = self
                    .ctx
                    .sql(sql)
                    .await
                    .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                self.plan_cache
                    .lock()
                    .insert(sql.to_string(), df.logical_plan().clone());
                df
            }
        };
        // Priority 8: record logical-planning time (parse + plan; ~0 on a
        // plan-cache hit), separate from execution.
        let planning_nanos = plan_start.elapsed().as_nanos() as u64;
        mongreldb_core::trace::QueryTrace::record(|t| t.planning_nanos = planning_nanos);

        // Phase 7.2/8.3 fast path: serve a simple single aggregate (SUM/MIN/MAX/
        // AVG/COUNT) over the primary table from the incremental aggregate
        // cache — warm cache ⇒ delta merge on commit; cold ⇒ vectorized scan.
        // Falls through to DataFusion for everything it cannot serve exactly.
        let agg_key = sql_cache_key(sql);
        let batches = match self.try_native_aggregate(df.logical_plan(), agg_key) {
            Ok(Some(batch)) => vec![batch],
            _ => {
                // Phase 8.1 fast path: serve a PK↔FK equi-join over two
                // registered tables via roaring-bitmap intersection, with no
                // hash-join materialization. Falls through otherwise.
                match self.try_fk_join(df.logical_plan()) {
                    Ok(Some(b)) => {
                        // Priority 13: the native FK-bitmap path served the join.
                        mongreldb_core::trace::QueryTrace::record(|t| {
                            t.join_mode = mongreldb_core::trace::JoinMode::FkBitmap;
                        });
                        b
                    }
                    _ => df
                        .collect()
                        .await
                        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?,
                }
            }
        };
        self.cache.lock().insert(key, Arc::new(batches.clone()));
        Ok(batches)
    }

    /// [`Self::run`] with a captured [`mongreldb_core::trace::QueryTrace`].
    ///
    /// Runs the SQL query inside a trace-capture scope so that path-decision
    /// recordings from both the SQL scan layer (`MongrelProvider::scan`) and
    /// the core engine (`Table::native_page_cursor`, `query_columns_native`,
    /// `count_conditions`, etc.) are collected into a single returned trace.
    ///
    /// The session-level result cache returns before `scan()` runs on a hit, so
    /// a session-cache hit yields `scan_mode = Unknown`. For scan-level
    /// result-cache tracing, use
    /// [`mongreldb_core::Table::query_columns_native_cached_traced`].
    pub async fn run_sql_traced(
        &self,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, mongreldb_core::trace::QueryTrace)> {
        mongreldb_core::trace::QueryTrace::push_scope();
        let result = self.run(sql).await;
        let trace = mongreldb_core::trace::QueryTrace::pop_scope();
        Ok((result?, trace))
    }

    /// Drop all cached results (e.g. after a manual data change you want
    /// reflected immediately).
    pub fn clear_cache(&self) {
        self.cache.lock().clear();
        self.plan_cache.lock().clear();
    }

    /// A cache key epoch combining the primary table's epoch with every
    /// secondary table's, so any registered table's commit invalidates cached
    /// results (correctness for multi-table joins).
    /// Phase 17.3: rewrite `FROM <view_name>` to `FROM (<view_sql>) AS <view_name>`.
    fn resolve_view_sql(&self, sql: &str) -> String {
        let views = self.views.lock();
        if views.is_empty() {
            return sql.to_string();
        }
        let mut result = sql.to_string();
        for (name, view_sql) in views.iter() {
            result = replace_from_view(&result, name, view_sql);
        }
        result
    }

    /// Cache epoch: uses `Database::visible_epoch()` when a Database is
    /// attached (P4.1), otherwise falls back to the legacy `combined_epoch()`.
    fn cache_epoch(&self) -> u64 {
        if let Some(db) = &self.database {
            db.visible_epoch().0
        } else {
            self.combined_epoch()
        }
    }

    fn combined_epoch(&self) -> u64 {
        let primary = self.db.as_ref().expect("no primary table");
        let mut combined = primary.lock().snapshot().epoch.0;
        let tables = self.tables.lock();
        for arc in tables.values() {
            if !Arc::ptr_eq(arc, primary) {
                let e = arc.lock().snapshot().epoch.0;
                combined = combined.wrapping_mul(31).wrapping_add(e);
            }
        }
        combined
    }

    /// Attempt the Phase 7.2/8.3 native aggregate fast path against `plan`.
    /// Returns `Ok(Some(batch))` when served natively, `Ok(None)` to fall
    /// through. `cache_key` ties the result to the incremental cache (Phase 8.3).
    fn try_native_aggregate(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
        cache_key: u64,
    ) -> Result<Option<RecordBatch>> {
        let Some(primary) = self.db.as_ref() else {
            return Ok(None);
        };
        let mut db = primary.lock();
        let schema = db.schema().clone();
        let snap = db.snapshot();
        native_agg::try_native_aggregate(&mut db, &schema, snap, plan, cache_key)
    }

    /// Attempt the Phase 8.1 FK-join (bitmap-intersection) fast path against
    /// `plan`. Returns `Ok(Some(batches))` when served natively, `Ok(None)` to
    /// fall through to DataFusion.
    fn try_fk_join(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
    ) -> Result<Option<Vec<RecordBatch>>> {
        let tables = self.tables.lock();
        fk_join::try_fk_join(&tables, plan)
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

/// Stable 64-bit cache key for a SQL string (Phase 8.3 incremental cache).
fn sql_cache_key(sql: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut h);
    h.finish()
}

/// Replace the first whole-word `FROM <name>` reference (case-insensitive) in
/// `sql` with `FROM (<view_sql>) AS <name>`. Unlike a raw substring search this
/// requires a word boundary on both sides, so a view named `log` will **not**
/// rewrite `FROM logs` (the prior behavior matched the `from log` prefix and
/// left a dangling `s`). Original (non-lowercased) casing is preserved outside
/// the rewritten span.
fn replace_from_view(sql: &str, name: &str, view_sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let name_b = name.as_bytes();
    let mut i = 0usize;
    while let Some(rel) = lower[i..].find("from") {
        let from_start = i + rel;
        let after_from = from_start + 4;
        i = after_from;
        // Left boundary: "from" must not be a suffix of a longer identifier.
        if from_start > 0 && is_ident_byte(bytes[from_start - 1]) {
            continue;
        }
        // Must be followed by whitespace then the name.
        let mut j = after_from;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j == after_from || !bytes[j..].starts_with(name_b) {
            continue;
        }
        let after_name = j + name_b.len();
        // Right boundary: the name must not be a prefix of a longer identifier.
        if after_name < bytes.len() && is_ident_byte(bytes[after_name]) {
            continue;
        }
        // Preserve the original `FROM ` casing/whitespace (sql[from_start..j]),
        // then wrap the view body as a subquery aliased back to the view name.
        let mut out = String::with_capacity(sql.len() + view_sql.len() + name.len() + 8);
        out.push_str(&sql[..from_start]);
        out.push_str(&sql[from_start..j]);
        out.push('(');
        out.push_str(view_sql);
        out.push_str(") AS ");
        out.push_str(name);
        out.push_str(&sql[after_name..]);
        return out;
    }
    sql.to_string()
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Canonicalize a SQL string for caching/parsing: collapse runs of ASCII
/// whitespace outside of literals/comments to a single space and trim. String
/// literals (`'...'`, with `''` escapes), quoted identifiers (`"..."`), escape
/// strings (`E'...'`), line comments (`--`), block comments (`/* */`), and
/// dollar-quoting (`$tag$...$tag$`) are passed through verbatim so their
/// internal whitespace (which IS semantically significant) is never altered.
/// SQL parsing is whitespace-insensitive outside literals, so the normalized
/// form parses identically while making `SELECT  *  FROM t`, `SELECT * FROM t`,
/// and `\n  SELECT * FROM t  \n` share one cache key.
fn normalize_sql(sql: &str) -> String {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out: Vec<u8> = Vec::with_capacity(n);
    // Whether a single separating space should precede the next emitted token
    // (i.e. we're between tokens, not at the very start of the output).
    let mut want_space = false;
    let mut i = 0usize;
    while i < n {
        let c = b[i];
        // Whitespace and comments both act only as token separators — they set
        // the pending-space flag but never emit a byte themselves, so a run of
        // "1  -- c\nFROM" collapses to a single separating space.
        if c.is_ascii_whitespace() {
            want_space = true;
            i += 1;
            continue;
        }
        if c == b'-' && i + 1 < n && b[i + 1] == b'-' {
            // Line comment: skip to end of line.
            i += 2;
            while i < n && b[i] != b'\n' {
                i += 1;
            }
            want_space = !out.is_empty();
            continue;
        }
        if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
            // Block comment: skip to the matching close `*/`, honoring nesting
            // (Postgres/DataFusion allow `/* /* */ */`).
            i += 2;
            let mut depth = 1usize;
            while i + 1 < n && depth > 0 {
                if b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            want_space = !out.is_empty();
            continue;
        }
        // A real token byte (or a literal/quote opener) — emit the separator.
        if want_space && !out.is_empty() {
            out.push(b' ');
        }
        want_space = false;
        match c {
            // Escape string E'...' (backslash escapes; '' is still an escape).
            b'E' | b'e' if i + 1 < n && b[i + 1] == b'\'' => {
                out.push(c);
                i += 1;
                i = copy_quoted(&mut out, b, i, n, b'\'');
                continue;
            }
            // Single-quoted string literal ('...' with '' escape).
            b'\'' => {
                i = copy_quoted(&mut out, b, i, n, b'\'');
                continue;
            }
            // Double-quoted identifier ("..." with "" escape).
            b'"' => {
                i = copy_quoted(&mut out, b, i, n, b'"');
                continue;
            }
            // Dollar-quoting: $tag$ ... $tag$ (tag optional/empty).
            b'$' => {
                let (consumed, matched) = copy_dollar_quoted(&mut out, b, i, n);
                if matched {
                    i = consumed;
                    continue;
                }
                out.push(c);
                i += 1;
                continue;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

/// Copy a quote-delimited span starting at `start` (the opening quote byte is
/// `delim`), including the opening and closing delimiters and any doubled
/// escapes, verbatim into `out`. Returns the index past the closing quote.
fn copy_quoted(out: &mut Vec<u8>, b: &[u8], start: usize, n: usize, delim: u8) -> usize {
    out.push(b[start]);
    let mut i = start + 1;
    while i < n {
        let c = b[i];
        out.push(c);
        if c == delim {
            // Doubled delimiter (e.g. '' or "") is an escape, not the end.
            if i + 1 < n && b[i + 1] == delim {
                out.push(b[i + 1]);
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    i
}

/// Copy a dollar-quoted span starting at the opening `$`. Returns
/// `(index_past_close, true)` if a matching close delimiter was found, or
/// `(start + 1, false)` if this `$` does not open a dollar-quote.
fn copy_dollar_quoted(out: &mut Vec<u8>, b: &[u8], start: usize, n: usize) -> (usize, bool) {
    // Parse the opening delimiter: '$' [tag] '$'. An empty tag ($$..$$) is
    // allowed; a non-empty tag must be identifier bytes starting with a
    // letter/underscore.
    let mut j = start + 1;
    let tag_start = j;
    while j < n && b[j] != b'$' && is_dollar_tag_byte(b[j]) {
        j += 1;
    }
    if j >= n || b[j] != b'$' {
        return (start + 1, false);
    }
    if tag_start < j && !(b[tag_start].is_ascii_alphabetic() || b[tag_start] == b'_') {
        return (start + 1, false);
    }
    let close_end = j + 1; // index just past the opening '$'
    let delim = &b[start..close_end];
    // Copy the opening delimiter verbatim.
    out.extend_from_slice(delim);
    // Find the matching close delimiter.
    let mut k = close_end;
    while k + delim.len() <= n {
        if &b[k..k + delim.len()] == delim {
            out.extend_from_slice(delim);
            return (k + delim.len(), true);
        }
        out.push(b[k]);
        k += 1;
    }
    // Unterminated: copy the remainder verbatim (don't corrupt).
    out.extend_from_slice(&b[close_end..n]);
    (n, true)
}

fn is_dollar_tag_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Strip an ASCII case-insensitive prefix from `s`, returning the remainder.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let bytes = s.as_bytes();
    let pb = prefix.as_bytes();
    if bytes.len() >= pb.len() && bytes[..pb.len()].eq_ignore_ascii_case(pb) {
        Some(&s[pb.len()..])
    } else {
        None
    }
}

/// Recognized column constraints in `CREATE TABLE` column definitions. Each
/// entry maps a SQL phrase (matched case-insensitively as a substring of the
/// whitespace-normalized constraint clause) to the [`ColumnFlags`] bit it sets.
///
/// Multi-word phrases such as `"primary key"` match regardless of internal
/// spacing because the clause is normalized to single spaces before matching.
///
/// **Adding a new column constraint is a one-line change:** append `(phrase,
/// flag)` here. This keeps the DDL shim's grammar in one place rather than
/// scattering `contains(...)` checks across the parser. (A full SQL grammar is
/// deliberately out of scope — only the DDL shapes handled below are
/// intercepted here; all query parsing is delegated to DataFusion.)
const COLUMN_CONSTRAINTS: &[(&str, u32)] = &[
    ("primary key", ColumnFlags::PRIMARY_KEY),
    // Both spellings are accepted: `AUTOINCREMENT` (SQLite) and `AUTO_INCREMENT`
    // (MySQL). The engine enforces that the flag is valid only on a single
    // non-nullable `Int64` primary key (see `Schema::validate_auto_increment`),
    // so recognizing the keyword on any column here is safe — invalid
    // placements are rejected at table-creation time, before the schema is
    // durably logged.
    ("autoincrement", ColumnFlags::AUTO_INCREMENT),
    ("auto_increment", ColumnFlags::AUTO_INCREMENT),
];

/// Translate a column's constraint clause (the text following `<name> <type>`
/// in a `CREATE TABLE` column definition) into [`ColumnFlags`]. The clause is
/// lowercased and its internal whitespace collapsed to single spaces so
/// multi-word phrases match regardless of formatting. See
/// [`COLUMN_CONSTRAINTS`] for the recognized phrases; add new ones there.
fn parse_column_constraints(constraint_text: &str) -> ColumnFlags {
    let normalized = constraint_text.to_lowercase();
    let mut flags = ColumnFlags::empty();
    for (phrase, bit) in COLUMN_CONSTRAINTS {
        if normalized.contains(phrase) {
            flags = flags.with(*bit);
        }
    }
    flags
}

fn parse_sql_type(ty_str: &str) -> Result<mongreldb_core::schema::TypeId> {
    use mongreldb_core::schema::TypeId;

    match ty_str.trim().trim_end_matches(';').to_lowercase().as_str() {
        "bigint" | "int8" | "int64" | "integer" | "int" => Ok(TypeId::Int64),
        "double" | "float8" | "float64" | "real" | "float" => Ok(TypeId::Float64),
        "varchar" | "text" | "string" | "bytes" => Ok(TypeId::Bytes),
        "boolean" | "bool" => Ok(TypeId::Bool),
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported column type: {other}"
        ))),
    }
}

/// Parse `CREATE TABLE [IF NOT EXISTS] <name> (<col> <type> <constraints>, ...)`
/// into a MongrelDB table name + schema. Supports BIGINT/INTEGER/INT, DOUBLE,
/// VARCHAR/TEXT, BOOLEAN. Recognized column constraints (`PRIMARY KEY`,
/// `AUTOINCREMENT` / `AUTO_INCREMENT`) are listed in [`COLUMN_CONSTRAINTS`].
/// Table name may be double-quoted.
fn parse_create_table(sql: &str) -> Result<(String, mongreldb_core::schema::Schema)> {
    use mongreldb_core::schema::*;

    let open = sql
        .find('(')
        .ok_or(MongrelQueryError::Schema("CREATE TABLE missing '('".into()))?;
    let close = sql
        .rfind(')')
        .ok_or(MongrelQueryError::Schema("CREATE TABLE missing ')'".into()))?;
    let head = sql[..open].trim();
    let after_kw = strip_prefix_ci(head, "CREATE TABLE")
        .or_else(|| strip_prefix_ci(head, "create table"))
        .unwrap_or("")
        .trim();
    // Skip optional `IF NOT EXISTS`.
    let after_kw = after_kw
        .strip_prefix("IF NOT EXISTS")
        .or_else(|| after_kw.strip_prefix("if not exists"))
        .map(str::trim)
        .unwrap_or(after_kw);
    let name = after_kw.trim_matches('"').to_string();
    if name.is_empty() {
        return Err(MongrelQueryError::Schema(
            "CREATE TABLE missing table name".into(),
        ));
    }

    let body = &sql[open + 1..close];
    let mut columns = Vec::new();
    let schema_id: u64 = 0; // Database::create_table overrides with the table_id.
    for (i, raw) in body.split(',').enumerate() {
        let part = raw.trim();
        if part.is_empty() {
            continue;
        }
        let mut tokens = part.split_whitespace();
        let col_name = tokens
            .next()
            .ok_or(MongrelQueryError::Schema("missing column name".into()))?
            .trim_matches('"');
        let ty_str = tokens
            .next()
            .ok_or(MongrelQueryError::Schema("missing column type".into()))?
            .to_lowercase();
        let ty = parse_sql_type(&ty_str)?;
        // Everything after `<name> <type>` is the column's constraint clause
        // (e.g. `PRIMARY KEY`, `PRIMARY KEY AUTOINCREMENT`). The remaining
        // tokens are matched against `COLUMN_CONSTRAINTS`.
        let constraint_clause: String = tokens.collect::<Vec<_>>().join(" ");
        let flags = parse_column_constraints(&constraint_clause);
        columns.push(ColumnDef {
            id: (i + 1) as u16,
            name: col_name.to_string(),
            ty,
            flags,
        });
    }

    Ok((
        name,
        Schema {
            schema_id,
            columns,
            indexes: vec![],
            colocation: vec![],
        },
    ))
}

/// Parse `DROP TABLE [IF EXISTS] <name>`. Returns `(name, if_exists)`.
fn parse_drop_table(sql: &str) -> Result<(String, bool)> {
    let head = sql.trim();
    let after_kw = strip_prefix_ci(head, "DROP TABLE")
        .or_else(|| strip_prefix_ci(head, "drop table"))
        .unwrap_or("")
        .trim();
    // Detect optional `IF EXISTS`.
    let (rest, if_exists) = if let Some(r) = after_kw
        .strip_prefix("IF EXISTS")
        .or_else(|| after_kw.strip_prefix("if exists"))
        .map(str::trim)
    {
        (r, true)
    } else {
        (after_kw, false)
    };
    let name = rest.trim_matches(';').trim_matches('"').trim();
    if name.is_empty() {
        return Err(MongrelQueryError::Schema(
            "DROP TABLE missing table name".into(),
        ));
    }
    Ok((name.to_string(), if_exists))
}

enum ParsedAlterTable {
    RenameTable {
        old_name: String,
        new_name: String,
    },
    RenameColumn {
        table_name: String,
        column_name: String,
        new_name: String,
    },
    AlterColumnType {
        table_name: String,
        column_name: String,
        ty: mongreldb_core::schema::TypeId,
    },
    SetNotNull {
        table_name: String,
        column_name: String,
    },
    DropNotNull {
        table_name: String,
        column_name: String,
    },
}

fn current_column_flags(db: &Arc<Database>, table: &str, column: &str) -> Result<ColumnFlags> {
    let handle = db.table(table)?;
    let table = handle.lock();
    table
        .schema()
        .column(column)
        .map(|c| c.flags)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column}")))
}

fn parse_alter_table(sql: &str) -> Result<ParsedAlterTable> {
    let trimmed = strip_statement_semicolon(sql.trim());
    let after_kw = strip_prefix_ci(trimmed, "ALTER TABLE")
        .ok_or_else(|| MongrelQueryError::Schema("not an ALTER TABLE statement".into()))?
        .trim();
    let (table_name, rest) = take_sql_ident(after_kw, "ALTER TABLE missing table name")?;
    let rest = rest.trim();

    if let Some(after) = strip_prefix_ci(rest, "RENAME TO") {
        let new_name = parse_trailing_identifier(after, "ALTER TABLE missing new table name")?;
        return Ok(ParsedAlterTable::RenameTable {
            old_name: table_name,
            new_name,
        });
    }

    if let Some(after) = strip_prefix_ci(rest, "RENAME COLUMN") {
        let (column_name, after_col) =
            take_sql_ident(after, "ALTER TABLE RENAME COLUMN missing column name")?;
        let after_to = strip_prefix_ci(after_col.trim(), "TO").ok_or_else(|| {
            MongrelQueryError::Schema("ALTER TABLE RENAME COLUMN missing TO".into())
        })?;
        let new_name = parse_trailing_identifier(
            after_to,
            "ALTER TABLE RENAME COLUMN missing new column name",
        )?;
        return Ok(ParsedAlterTable::RenameColumn {
            table_name,
            column_name,
            new_name,
        });
    }

    let after_alter = strip_prefix_ci(rest, "ALTER COLUMN")
        .or_else(|| strip_prefix_ci(rest, "ALTER"))
        .ok_or_else(|| {
            MongrelQueryError::Schema(
                "ALTER TABLE must be RENAME TO, RENAME COLUMN, or ALTER COLUMN".into(),
            )
        })?;
    let (column_name, action) =
        take_sql_ident(after_alter, "ALTER TABLE ALTER COLUMN missing column name")?;
    let action = action.trim();

    if let Some(after_type) =
        strip_prefix_ci(action, "TYPE").or_else(|| strip_prefix_ci(action, "SET DATA TYPE"))
    {
        let ty = parse_type_tail(after_type)?;
        return Ok(ParsedAlterTable::AlterColumnType {
            table_name,
            column_name,
            ty,
        });
    }
    if strip_prefix_ci(action, "SET NOT NULL").is_some() {
        return Ok(ParsedAlterTable::SetNotNull {
            table_name,
            column_name,
        });
    }
    if strip_prefix_ci(action, "DROP NOT NULL").is_some() {
        return Ok(ParsedAlterTable::DropNotNull {
            table_name,
            column_name,
        });
    }

    Err(MongrelQueryError::Schema(
        "unsupported ALTER COLUMN action".into(),
    ))
}

fn strip_statement_semicolon(s: &str) -> &str {
    s.trim().trim_end_matches(';').trim()
}

fn take_sql_ident<'a>(s: &'a str, missing: &str) -> Result<(String, &'a str)> {
    let s = s.trim();
    if s.is_empty() {
        return Err(MongrelQueryError::Schema(missing.into()));
    }
    if let Some(rest) = s.strip_prefix('"') {
        let Some(end) = rest.find('"') else {
            return Err(MongrelQueryError::Schema(
                "unterminated quoted identifier".into(),
            ));
        };
        let ident = rest[..end].to_string();
        if ident.is_empty() {
            return Err(MongrelQueryError::Schema(missing.into()));
        }
        return Ok((ident, &rest[end + 1..]));
    }
    let end = s.find(|c: char| c.is_ascii_whitespace()).unwrap_or(s.len());
    let ident = s[..end].trim_matches('"').to_string();
    if ident.is_empty() {
        return Err(MongrelQueryError::Schema(missing.into()));
    }
    Ok((ident, &s[end..]))
}

fn parse_trailing_identifier(s: &str, missing: &str) -> Result<String> {
    let (ident, rest) = take_sql_ident(s, missing)?;
    if !strip_statement_semicolon(rest).is_empty() {
        return Err(MongrelQueryError::Schema(
            "unexpected tokens after identifier".into(),
        ));
    }
    Ok(ident)
}

fn parse_type_tail(s: &str) -> Result<mongreldb_core::schema::TypeId> {
    let tail = strip_statement_semicolon(s);
    let ty = tail
        .split_whitespace()
        .next()
        .ok_or_else(|| MongrelQueryError::Schema("ALTER COLUMN TYPE missing type".into()))?;
    parse_sql_type(ty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_collapses_and_trims_whitespace() {
        assert_eq!(normalize_sql("SELECT * FROM t"), "SELECT * FROM t");
        assert_eq!(normalize_sql("  SELECT  *   FROM   t  "), "SELECT * FROM t");
        assert_eq!(
            normalize_sql("\n\tSELECT\n*\nFROM\n\tt\n"),
            "SELECT * FROM t"
        );
        assert_eq!(
            normalize_sql("SELECT   a,   b   FROM   t"),
            normalize_sql("SELECT a, b FROM t")
        );
    }

    #[test]
    fn normalize_preserves_string_literal_whitespace() {
        assert_eq!(
            normalize_sql("SELECT 'hello   world' FROM t"),
            "SELECT 'hello   world' FROM t"
        );
        assert_eq!(
            normalize_sql("SELECT 'it''s   ok' FROM t"),
            "SELECT 'it''s   ok' FROM t"
        );
        assert_eq!(
            normalize_sql("  SELECT  'a  b'  FROM  t  "),
            "SELECT 'a  b' FROM t"
        );
    }

    #[test]
    fn normalize_preserves_quoted_identifier_and_dollar_quote() {
        assert_eq!(
            normalize_sql("  SELECT  \"my col\"  FROM  t  "),
            "SELECT \"my col\" FROM t"
        );
        assert_eq!(
            normalize_sql("  SELECT  $$a   b$$  FROM  t  "),
            "SELECT $$a   b$$ FROM t"
        );
        assert_eq!(
            normalize_sql("SELECT $tag$body   with spaces$tag$ FROM t"),
            "SELECT $tag$body   with spaces$tag$ FROM t"
        );
    }

    #[test]
    fn normalize_strips_comments() {
        assert_eq!(
            normalize_sql("SELECT 1 -- trailing comment\nFROM t"),
            "SELECT 1 FROM t"
        );
        assert_eq!(
            normalize_sql("SELECT /* block */ 1 FROM t"),
            "SELECT 1 FROM t"
        );
        // Comment with a quote-like body must not confuse the scanner.
        assert_eq!(
            normalize_sql("SELECT /* 'not a string' */ 1 FROM t"),
            "SELECT 1 FROM t"
        );
        // Nested block comments are honored (Postgres/DataFusion allow nesting).
        assert_eq!(
            normalize_sql("SELECT /* outer /* inner */ still outer */ 1 FROM t"),
            "SELECT 1 FROM t"
        );
    }

    #[test]
    fn normalize_escape_string_preserved() {
        assert_eq!(
            normalize_sql("SELECT E'line\\nbreak' FROM t"),
            "SELECT E'line\\nbreak' FROM t"
        );
    }

    #[test]
    fn replace_from_view_matches_whole_word_only() {
        let out = replace_from_view("SELECT * FROM logs", "log", "SELECT 1");
        assert_eq!(out, "SELECT * FROM logs");

        let out = replace_from_view("SELECT * FROM log", "log", "SELECT 1");
        assert_eq!(out, "SELECT * FROM (SELECT 1) AS log");

        let out = replace_from_view("select * from log where x", "log", "SELECT 1");
        assert_eq!(out, "select * from (SELECT 1) AS log where x");

        let out = replace_from_view("SELECT * FROM log)", "log", "SELECT 1");
        assert_eq!(out, "SELECT * FROM (SELECT 1) AS log)");

        let out = replace_from_view("SELECT * xfrom log", "log", "SELECT 1");
        assert_eq!(out, "SELECT * xfrom log");
    }
}
