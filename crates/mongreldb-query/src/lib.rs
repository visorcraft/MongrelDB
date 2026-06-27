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
use mongreldb_core::{Cursor, Table};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A MongrelDB table exposed to DataFusion. Holds the live `Table` behind a mutex;
/// each scan takes a fresh MVCC snapshot.
pub struct MongrelProvider {
    db: Arc<Mutex<Table>>,
    schema: SchemaRef,
}

impl MongrelProvider {
    pub fn new(db: Arc<Mutex<Table>>) -> Result<Self> {
        let schema = {
            let db = db.lock().expect("db mutex poisoned");
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
        let schema_ref = self.db.lock().expect("db mutex poisoned").schema().clone();
        Ok(filters
            .iter()
            .map(|f| match translate_filter(f, &schema_ref) {
                Some(mongreldb_core::Condition::FmContains { .. }) => {
                    TableProviderFilterPushDown::Inexact
                }
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
        let mut db = self.db.lock().expect("db mutex poisoned");
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
                db.count() as usize
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
        // projected columns of surviving pages. Single-run uses the page-plan
        // fast path; multi-run uses the k-way-merge cursor (Phase 16.1) — both
        // avoid fully materializing every row. Anything else (e.g. an empty
        // table with only memtable rows) falls through to materialize-then-chunk.
        let cursor: Option<Box<dyn Cursor>> = if db.run_count() == 1 {
            db.native_page_cursor(snap, proj_pairs, &translated)
                .map_err(core_err)?
                .map(|c| Box::new(c) as Box<dyn Cursor>)
        } else {
            db.native_multi_run_cursor(snap, proj_pairs, &translated)
                .map_err(core_err)?
                .map(|c| Box::new(c) as Box<dyn Cursor>)
        };
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

    let int_val = |s: &ScalarValue| match s {
        ScalarValue::Int64(Some(v)) => Some(*v),
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
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
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
            longest_like_segment(pat).map(|seg| Condition::FmContains {
                column_id: cdef.id,
                pattern: seg,
            })
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

/// Convenience wrapper: a DataFusion `SessionContext` bound to a live MongrelDB,
/// with a result cache keyed by `(sql, snapshot_epoch)` that auto-invalidates
/// when a commit advances the epoch.
pub struct MongrelSession {
    ctx: SessionContext,
    db: Arc<Mutex<Table>>,
    cache: ResultCache,
    /// Phase 16.5: logical-plan cache keyed by SQL string.
    plan_cache: std::sync::Mutex<HashMap<String, datafusion::logical_expr::LogicalPlan>>,
    /// `table name → owning Table handle` for every registered table.
    tables: std::sync::Mutex<HashMap<String, Arc<Mutex<Table>>>>,
    /// Phase 17.3: named materialized views — `view name → defining SQL`.
    /// On `run("SELECT * FROM <view>")`, the defining SQL is executed (or the
    /// result-cache is hit). Invalidated automatically on commit (epoch bump).
    views: std::sync::Mutex<HashMap<String, String>>,
}

/// `(sql, snapshot_epoch) → cached result batches`.
type CacheKey = (String, u64);
type ResultCache = std::sync::Mutex<std::collections::HashMap<CacheKey, Arc<Vec<RecordBatch>>>>;

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
            db,
            cache: std::sync::Mutex::new(std::collections::HashMap::new()),
            plan_cache: std::sync::Mutex::new(HashMap::new()),
            tables: std::sync::Mutex::new(HashMap::new()),
            views: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The underlying Table handle (Phase 19.3: used by the daemon for direct
    /// put/delete/commit/count access).
    pub fn db(&self) -> &Arc<Mutex<Table>> {
        &self.db
    }

    /// Phase 17.3: create a named materialized view backed by a SQL query.
    /// `SELECT * FROM <name>` resolves to the view's defining SQL, which is
    /// executed (or served from the result cache) transparently. The view is
    /// automatically invalidated on commit (via the epoch-keyed result cache).
    pub fn create_view(&self, name: &str, sql: &str) {
        self.views
            .lock()
            .unwrap()
            .insert(name.to_string(), sql.to_string());
    }

    /// Drop a named materialized view.
    pub fn drop_view(&self, name: &str) {
        self.views.lock().unwrap().remove(name);
    }

    /// Register the table under `name` so `select * from <name>` resolves.
    pub async fn register(&self, name: &str) -> Result<()> {
        let provider = MongrelProvider::new(self.db.clone())?;
        self.tables
            .lock()
            .unwrap()
            .insert(name.to_string(), self.db.clone());
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
        self.tables.lock().unwrap().insert(name.to_string(), db_arc);
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        Ok(())
    }

    /// Run a SQL statement and return the result batches. Repeated identical SQL
    /// against the same snapshot returns the cached batches without re-executing.
    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Phase 17.3: intercept `SELECT ... FROM <view_name>` and rewrite to
        // the view's defining SQL.
        let effective_sql = self.resolve_view_sql(sql);
        let sql = effective_sql.as_str();
        // The cache key folds in every registered table's epoch, not just the
        // primary's, so a commit on a secondary (join) table invalidates cached
        // multi-table results (Phase 8 review fix).
        let epoch = self.combined_epoch();
        let key = (sql.to_string(), epoch);
        if let Some(hit) = self.cache.lock().unwrap().get(&key) {
            return Ok((**hit).clone());
        }
        // Phase 16.5: check the logical-plan cache before re-parsing.
        let df = {
            let cached_plan = self.plan_cache.lock().unwrap().get(sql).cloned();
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
                    .unwrap()
                    .insert(sql.to_string(), df.logical_plan().clone());
                df
            }
        };

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
                    Ok(Some(b)) => b,
                    _ => df
                        .collect()
                        .await
                        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?,
                }
            }
        };
        self.cache
            .lock()
            .unwrap()
            .insert(key, Arc::new(batches.clone()));
        Ok(batches)
    }

    /// Drop all cached results (e.g. after a manual data change you want
    /// reflected immediately).
    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
        self.plan_cache.lock().unwrap().clear();
    }

    /// A cache key epoch combining the primary table's epoch with every
    /// secondary table's, so any registered table's commit invalidates cached
    /// results (correctness for multi-table joins).
    /// Phase 17.3: rewrite `FROM <view_name>` to `FROM (<view_sql>) AS <view_name>`.
    fn resolve_view_sql(&self, sql: &str) -> String {
        let views = self.views.lock().unwrap();
        if views.is_empty() {
            return sql.to_string();
        }
        let mut result = sql.to_string();
        for (name, view_sql) in views.iter() {
            let pattern = format!("from {name}");
            let lower_pattern = pattern.to_lowercase();
            // Recompute the lowercase search each iteration — the result may
            // have changed length after a prior replacement.
            let lower = result.to_lowercase();
            if let Some(pos) = lower.find(&lower_pattern) {
                let replacement = format!("FROM ({view_sql}) AS {name}");
                result = format!(
                    "{}{}{}",
                    &result[..pos],
                    replacement,
                    &result[pos + pattern.len()..]
                );
            }
        }
        result
    }

    fn combined_epoch(&self) -> u64 {
        let mut combined = self
            .db
            .lock()
            .expect("db mutex poisoned")
            .snapshot()
            .epoch
            .0;
        let tables = self.tables.lock().unwrap();
        for arc in tables.values() {
            if !Arc::ptr_eq(arc, &self.db) {
                let e = arc.lock().expect("db mutex poisoned").snapshot().epoch.0;
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
        let mut db = self.db.lock().expect("db mutex poisoned");
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
        let tables = self.tables.lock().unwrap();
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
