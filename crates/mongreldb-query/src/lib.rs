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
pub mod extended_sql_functions;
mod external_modules;
mod fk_join;
mod native_agg;
mod percentile;
mod scan;
mod scored_sql;
mod shadow;
mod udf;

pub use error::{MongrelQueryError, Result};
pub use external_modules::{
    ExternalBaseWrite, ExternalModuleDescriptor, ExternalModuleIndex, ExternalModuleRegistry,
    ExternalPlan, ExternalPlanRequest, ExternalScan, ExternalTable, ExternalTableModule,
    ExternalTxn, ExternalWriteOp, ExternalWriteResult, ModuleConnectCtx,
};

pub type MongrelRecordBatchStream = datafusion::physical_plan::SendableRecordBatchStream;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{AggregateUDF, Expr, ScalarUDF, TableType, WindowUDF};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use mongreldb_core::{
    AlterColumn, ColumnFlags, Cursor, Database, OwnedSnapshotGuard, Schema as CoreSchema, Snapshot,
    Table, TypeId,
};
use parking_lot::Mutex;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

/// A MongrelDB table exposed to DataFusion. Holds the live `Table` behind a mutex;
/// each scan takes a fresh MVCC snapshot.
pub struct MongrelProvider {
    db: Arc<Mutex<Table>>,
    schema: SchemaRef,
    core_schema: CoreSchema,
    snapshot: Option<Snapshot>,
    _retention: Option<Arc<OwnedSnapshotGuard>>,
    security: Option<ProviderSecurity>,
}

#[derive(Clone)]
struct ProviderSecurity {
    database: Arc<Database>,
    table: String,
    principal: Option<mongreldb_core::Principal>,
    principal_catalog_bound: bool,
}

impl ProviderSecurity {
    fn principal(&self) -> mongreldb_core::Result<Option<mongreldb_core::Principal>> {
        let Some(principal) = &self.principal else {
            return Ok(None);
        };
        if self.principal_catalog_bound {
            self.database
                .resolve_principal(&principal.username)
                .map(Some)
                .ok_or(mongreldb_core::MongrelError::AuthRequired)
        } else {
            Ok(Some(principal.clone()))
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ViewDef {
    pub sql: String,
    pub schema: CoreSchema,
    pub input_types: HashMap<u16, Option<TypeId>>,
}

impl MongrelProvider {
    pub fn new(db: Arc<Mutex<Table>>) -> Result<Self> {
        let (schema, core_schema) = {
            let db = db.lock();
            (arrow_conv::arrow_schema(db.schema())?, db.schema().clone())
        };
        Ok(Self {
            db,
            schema,
            core_schema,
            snapshot: None,
            _retention: None,
            security: None,
        })
    }

    pub(crate) fn new_secured(
        db: Arc<Mutex<Table>>,
        database: Arc<Database>,
        table: String,
        principal: Option<mongreldb_core::Principal>,
    ) -> Result<Self> {
        let core_schema = db.lock().schema().clone();
        let schema = arrow_conv::arrow_schema(&core_schema)?;
        Ok(Self {
            db,
            schema,
            core_schema,
            snapshot: None,
            _retention: None,
            security: Some(ProviderSecurity {
                principal_catalog_bound: principal.as_ref().is_some_and(|principal| {
                    database.resolve_principal(&principal.username).is_some()
                }),
                database,
                table,
                principal,
            }),
        })
    }

    fn new_historical(
        db: Arc<Mutex<Table>>,
        snapshot: Snapshot,
        retention: OwnedSnapshotGuard,
        security: Option<ProviderSecurity>,
    ) -> Result<Self> {
        let full_schema = {
            let db = db.lock();
            db.schema().clone()
        };
        let core_schema = full_schema;
        let schema = arrow_conv::arrow_schema(&core_schema)?;
        Ok(Self {
            db,
            schema,
            core_schema,
            snapshot: Some(snapshot),
            _retention: Some(Arc::new(retention)),
            security,
        })
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

struct AsOfRegistration {
    ctx: SessionContext,
    table_name: String,
}

impl Drop for AsOfRegistration {
    fn drop(&mut self) {
        let _ = self.ctx.deregister_table(&self.table_name);
    }
}

struct AsOfQuery {
    sql: String,
    registration: AsOfRegistration,
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
        if self.snapshot.is_some() {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }
        let schema_ref = self.db.lock().schema().clone();
        if self.security.is_some() {
            return Ok(filters
                .iter()
                .map(|filter| {
                    if translate_ann_search(filter, &schema_ref).is_some()
                        || translate_sparse_match(filter, &schema_ref).is_some()
                    {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                })
                .collect());
        }
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
        if let Some(security) = &self.security {
            let principal = security.principal().map_err(core_err)?;
            let allowed = security
                .database
                .select_column_ids_for(&security.table, principal.as_ref())
                .map_err(core_err)?;
            let projected = projection
                .map(|projection| {
                    projection
                        .iter()
                        .filter_map(|index| self.core_schema.columns.get(*index))
                        .map(|column| column.id)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| {
                    self.core_schema
                        .columns
                        .iter()
                        .map(|column| column.id)
                        .collect()
                });
            if projected.iter().any(|column| !allowed.contains(column)) {
                security
                    .database
                    .require_columns_for(
                        &security.table,
                        mongreldb_core::ColumnOperation::Select,
                        &projected,
                        principal.as_ref(),
                    )
                    .map_err(core_err)?;
            }
            let ai_conditions = filters
                .iter()
                .filter_map(|filter| {
                    translate_ann_search(filter, &self.core_schema)
                        .or_else(|| translate_sparse_match(filter, &self.core_schema))
                })
                .collect::<Vec<_>>();
            let mut required_columns = projected.clone();
            required_columns.extend(mongreldb_core::query::condition_columns(&ai_conditions));
            required_columns.sort_unstable();
            required_columns.dedup();
            let rows = if let Some(snapshot) = self.snapshot {
                let rows = self.db.lock().visible_rows(snapshot).map_err(core_err)?;
                security
                    .database
                    .secure_rows_for(&security.table, rows, principal.as_ref())
                    .map_err(core_err)?
            } else {
                security
                    .database
                    .with_authorized_read(
                        &security.table,
                        principal.as_ref(),
                        security.principal_catalog_bound,
                        |table, snapshot, allowed, effective_principal| {
                            security.database.require_columns_for(
                                &security.table,
                                mongreldb_core::ColumnOperation::Select,
                                &required_columns,
                                effective_principal,
                            )?;
                            let rows = if ai_conditions.is_empty() {
                                let mut rows = table.visible_rows(snapshot)?;
                                if let Some(allowed) = allowed {
                                    rows.retain(|row| allowed.contains(&row.row_id));
                                }
                                rows
                            } else {
                                table.query_at_with_allowed(
                                    &mongreldb_core::Query {
                                        conditions: ai_conditions.clone(),
                                        limit: Some(mongreldb_core::query::MAX_FINAL_LIMIT),
                                        offset: 0,
                                    },
                                    snapshot,
                                    allowed,
                                )?
                            };
                            security.database.secure_rows_for(
                                &security.table,
                                rows,
                                effective_principal,
                            )
                        },
                    )
                    .map_err(core_err)?
            };
            if projection.is_some_and(Vec::is_empty) {
                return Ok(Arc::new(scan::MongrelScanExec::new_row_count(rows.len())));
            }
            let batch = arrow_conv::rows_to_batch(&rows, &self.core_schema)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let batch = match projection {
                Some(projection) => batch.project(projection).map_err(|error| {
                    DataFusionError::External(Box::new(MongrelQueryError::Arrow(error.to_string())))
                })?,
                None => batch,
            };
            let schema = batch.schema();
            let statistics = (0..batch.num_columns())
                .map(|_| scan::to_col_statistics(None))
                .collect();
            return Ok(Arc::new(scan::MongrelScanExec::new_batch(
                schema, batch, statistics,
            )));
        }
        let mut db = self.db.lock();
        // Enforce Select permission before any read path (count metadata,
        // count_conditions, or full scan_cursor). On a credentialless database
        // this is a no-op.
        db.require_select().map_err(core_err)?;
        let historical = self.snapshot.is_some();
        let snap = self.snapshot.unwrap_or_else(|| db.snapshot());
        let schema_ref = db.schema().clone();

        // Translate WHERE filters into index-backed Conditions.
        let translated: Vec<mongreldb_core::Condition> = if historical {
            Vec::new()
        } else {
            filters
                .iter()
                .filter_map(|f| {
                    translate_filter(f, &schema_ref)
                        .or_else(|| translate_ann_search(f, &schema_ref))
                        .or_else(|| translate_sparse_match(f, &schema_ref))
                })
                .collect()
        };

        // Index-served conditions require complete live indexes; a deferred
        // bulk load pays its one-time build here (Phase 14.7 lazy contract).
        if !translated.is_empty() {
            db.ensure_indexes_complete().map_err(core_err)?;
        }

        // `COUNT(*)`-style queries (empty projection) need only a row count.
        // Unfiltered ⇒ O(1) via the maintained `live_count` metadata; a pushed
        // WHERE ⇒ decode one column through the pushdown path to count survivors.
        let empty_proj = projection.map(|p| p.is_empty()).unwrap_or(false);
        if empty_proj {
            let total: usize = if historical {
                db.visible_rows(snap).map_err(core_err)?.len()
            } else if translated.is_empty() {
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
                .map(|c| c.ty.clone())
                .ok_or_else(|| {
                    DataFusionError::External(Box::new(MongrelQueryError::Arrow(format!(
                        "unknown column {cid}"
                    ))))
                })?;
            proj_pairs.push((*cid, ty.clone()));
            types.push(ty);
        }

        // Phase 7.1: exact per-column min/max from page stats, but only for an
        // unfiltered full scan over an insert-only table (gated in core). A
        // pushed WHERE or a table with deletes ⇒ all-Absent (DataFusion scans).
        let col_stats_map = if !historical && translated.is_empty() {
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
        if !historical
            && translated.is_empty()
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
        )> = if !historical
            && translated.is_empty()
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
                        .map(|(col, ty)| arrow_conv::native_to_array(ty.clone(), col))
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

    // Extended int extraction: handles every integer width (narrow ints are
    // stored widened to Int64 internally), Date32, and all Timestamp* precision
    // variants DataFusion emits. The numeric value is the raw i64.
    let int_val = |s: &ScalarValue| match s {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        ScalarValue::Date32(Some(v)) => Some(*v as i64),
        ScalarValue::TimestampSecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        _ => None,
    };
    let float_val = |s: &ScalarValue| match s {
        ScalarValue::Float32(Some(f)) => Some(*f as f64),
        ScalarValue::Float64(Some(f)) => Some(*f),
        _ => None,
    };
    let bytes_val = |s: &ScalarValue| match s {
        ScalarValue::Utf8(Some(s)) => Some(s.as_bytes().to_vec()),
        _ => None,
    };
    let _ = bytes_val; // retained for clarity; equality uses the generic `val` below.

    let val = |s: &ScalarValue| -> Option<Value> {
        // Integer literals of any width coerce to Int64 (the storage width);
        // Float32 widens to Float64. This keeps equality pushdown working on
        // narrow-int / float32 bitmap and primary-key columns.
        if let Some(i) = int_val(s) {
            return Some(Value::Int64(i));
        }
        match s {
            ScalarValue::Utf8(Some(s)) => Some(Value::Bytes(s.as_bytes().to_vec())),
            ScalarValue::Float32(Some(f)) => Some(Value::Float64(*f as f64)),
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

            // Range on a typed numeric column. Every integer width is stored
            // widened to Int64, so they all share the integer Range path.
            match cdef.ty {
                TypeId::Int8
                | TypeId::Int16
                | TypeId::Int32
                | TypeId::Int64
                | TypeId::UInt8
                | TypeId::UInt16
                | TypeId::UInt32
                | TypeId::UInt64
                | TypeId::TimestampNanos
                | TypeId::Date32 => {
                    let v = int_val(scalar)?;
                    let (lo, hi) = int_bounds(op, v)?;
                    Some(Condition::Range {
                        column_id: cdef.id,
                        lo,
                        hi,
                    })
                }
                TypeId::Float32 | TypeId::Float64 => {
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
                TypeId::Int8
                | TypeId::Int16
                | TypeId::Int32
                | TypeId::Int64
                | TypeId::UInt8
                | TypeId::UInt16
                | TypeId::UInt32
                | TypeId::UInt64
                | TypeId::TimestampNanos
                | TypeId::Date32 => {
                    let (Some(lo), Some(hi)) = (int_val(lo_s), int_val(hi_s)) else {
                        return None;
                    };
                    Some(Condition::Range {
                        column_id: cdef.id,
                        lo,
                        hi,
                    })
                }
                TypeId::Float32 | TypeId::Float64 => {
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
            // §5.6: anchored prefix `LIKE 'literal%'` (no embedded wildcards)
            // on a bitmap-indexed column → exact BytesPrefix, tighter than the
            // FM substring superset. Checked before the FM path.
            if has_bitmap(cdef.id) {
                if let Some(prefix) = anchored_like_prefix(pat) {
                    return Some(Condition::BytesPrefix {
                        column_id: cdef.id,
                        prefix: mongreldb_core::Value::Bytes(prefix.as_bytes().to_vec())
                            .encode_key(),
                    });
                }
            }
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
    let k: usize = match k_expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int64(Some(k)) => usize::try_from(*k).ok()?,
            ScalarValue::UInt64(Some(k)) => usize::try_from(*k).ok()?,
            ScalarValue::Int32(Some(k)) => usize::try_from(*k).ok()?,
            _ => return None,
        },
        _ => return None,
    };
    let query: Vec<f32> = serde_json::from_str(json).ok()?;
    Some(Condition::Ann {
        column_id: cdef.id,
        query,
        k,
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
    let k: usize = match k_expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int64(Some(k)) => usize::try_from(*k).ok()?,
            ScalarValue::UInt64(Some(k)) => usize::try_from(*k).ok()?,
            ScalarValue::Int32(Some(k)) => usize::try_from(*k).ok()?,
            _ => return None,
        },
        _ => return None,
    };
    let query: Vec<(u32, f32)> = serde_json::from_str(json).ok()?;
    Some(Condition::SparseMatch {
        column_id: cdef.id,
        query,
        k,
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

/// Detect an anchored-prefix LIKE pattern: `literal%` with no `%` or `_` in
/// the literal part and a single trailing `%`. Returns the prefix (without the
/// `%`). Used to emit an exact `BytesPrefix` condition on bitmap-indexed
/// columns — tighter than the FM substring superset. (§5.6)
fn anchored_like_prefix(pat: &str) -> Option<&str> {
    let rest = pat.strip_suffix('%')?;
    if rest.is_empty() || rest.contains(['%', '_']) {
        return None;
    }
    Some(rest)
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

// ──────────────────────────────────────────────────────────────────────────
// §5.3 direct SQL dispatch: translate a sqlparser AST WHERE clause into the
// engine's exact Condition set (no DataFusion involvement). Only predicates
// whose Condition is EXACT are accepted; everything else returns None so the
// caller falls through to the DataFusion path (which re-applies residuals).

fn sp_ident_name(expr: &sqlparser::ast::Expr) -> Option<&str> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// A sqlparser literal → core Value. Numbers widen to Int64 (or Float64 if they
/// don't fit i64); single-quoted strings → Bytes; booleans → Bool.
fn sp_literal(expr: &sqlparser::ast::Expr) -> Option<mongreldb_core::Value> {
    use sqlparser::ast::Expr;
    let v = match expr {
        Expr::Value(v) => v,
        _ => return None,
    };
    use sqlparser::ast::Value as SpValue;
    match &v.value {
        SpValue::Number(s, _) => s
            .parse::<i64>()
            .map(mongreldb_core::Value::Int64)
            .or_else(|_| s.parse::<f64>().map(mongreldb_core::Value::Float64))
            .ok(),
        SpValue::SingleQuotedString(s) => Some(mongreldb_core::Value::Bytes(s.as_bytes().to_vec())),
        SpValue::Boolean(b) => Some(mongreldb_core::Value::Bool(*b)),
        _ => None,
    }
}

fn is_int_ty(ty: mongreldb_core::schema::TypeId) -> bool {
    use mongreldb_core::schema::TypeId::*;
    matches!(
        ty,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | TimestampNanos | Date32
    )
}

fn is_float_ty(ty: mongreldb_core::schema::TypeId) -> bool {
    matches!(
        ty,
        mongreldb_core::schema::TypeId::Float32 | mongreldb_core::schema::TypeId::Float64
    )
}

/// Translate ONE sqlparser predicate `Expr` into one exact `Condition`.
/// Returns `None` for anything inexact or unsupported (→ caller falls through).
fn translate_sqlparser_predicate(
    expr: &sqlparser::ast::Expr,
    schema: &mongreldb_core::Schema,
) -> Option<mongreldb_core::Condition> {
    use mongreldb_core::{schema::ColumnFlags, Condition, IndexKind, Value};
    use sqlparser::ast::{BinaryOperator, Expr};

    let col_def = |name: &str| schema.columns.iter().find(|c| c.name == name);
    let has_bitmap = |cid: u16| {
        schema
            .indexes
            .iter()
            .any(|i| i.column_id == cid && i.kind == IndexKind::Bitmap)
    };

    match expr {
        // `a = b OR a = c …` (one column, all literals) → BitmapIn.
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let mut values: Vec<Vec<u8>> = Vec::new();
            let mut target: Option<u16> = None;
            let mut stack: Vec<&Expr> = vec![left.as_ref(), right.as_ref()];
            while let Some(e) = stack.pop() {
                match e {
                    Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Or,
                        right,
                    } => {
                        stack.push(left.as_ref());
                        stack.push(right.as_ref());
                    }
                    Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        right,
                    } => {
                        let (name, lit) = match (left.as_ref(), right.as_ref()) {
                            (l, r) if sp_ident_name(l).is_some() && sp_literal(r).is_some() => {
                                (l, r)
                            }
                            (l, r) if sp_ident_name(r).is_some() && sp_literal(l).is_some() => {
                                (r, l)
                            }
                            _ => return None,
                        };
                        let cdef = col_def(sp_ident_name(name)?)?;
                        if !has_bitmap(cdef.id) {
                            return None;
                        }
                        match target {
                            None => target = Some(cdef.id),
                            Some(id) if id != cdef.id => return None,
                            _ => {}
                        }
                        values.push(sp_literal(lit)?.encode_key());
                    }
                    _ => return None,
                }
            }
            let cid = target?;
            (!values.is_empty()).then_some(Condition::BitmapIn {
                column_id: cid,
                values,
            })
        }
        // Comparison `col OP literal` (or mirrored).
        Expr::BinaryOp { left, op, right } => {
            let flipped;
            let (col_expr, lit_expr) = match (
                sp_ident_name(left),
                sp_literal(right),
                sp_ident_name(right),
                sp_literal(left),
            ) {
                (Some(_), Some(_), _, _) => {
                    flipped = false;
                    (left.as_ref(), right.as_ref())
                }
                (_, _, Some(_), Some(_)) => {
                    flipped = true;
                    (right.as_ref(), left.as_ref())
                }
                _ => return None,
            };
            let name = sp_ident_name(col_expr)?;
            let cdef = col_def(name)?;
            let v = sp_literal(lit_expr)?;
            use sqlparser::ast::BinaryOperator::*;
            // Inline the comparison→Range/RangeF64 bounds, fusing the flip
            // (BinaryOperator is not Copy, so we match the &op directly).
            match op {
                Eq => {
                    if has_bitmap(cdef.id) {
                        Some(Condition::BitmapEq {
                            column_id: cdef.id,
                            value: v.encode_key(),
                        })
                    } else if cdef.flags.contains(ColumnFlags::PRIMARY_KEY) {
                        Some(Condition::Pk(v.encode_key()))
                    } else {
                        None
                    }
                }
                Lt | LtEq | Gt | GtEq if is_int_ty(cdef.ty.clone()) => {
                    let n = match v {
                        Value::Int64(n) => n,
                        _ => return None,
                    };
                    // `col OP v`, or the mirrored `v OP col` with the flipped op.
                    let (lo, hi) = match (flipped, op) {
                        (false, Lt) | (true, Gt) => (i64::MIN, n.saturating_sub(1)),
                        (false, Gt) | (true, Lt) => (n.saturating_add(1), i64::MAX),
                        (false, LtEq) | (true, GtEq) => (i64::MIN, n),
                        (false, GtEq) | (true, LtEq) => (n, i64::MAX),
                        _ => (i64::MIN, i64::MAX),
                    };
                    Some(Condition::Range {
                        column_id: cdef.id,
                        lo,
                        hi,
                    })
                }
                Lt | LtEq | Gt | GtEq if is_float_ty(cdef.ty.clone()) => {
                    let f = match v {
                        Value::Float64(f) => f,
                        _ => return None,
                    };
                    let (lo, li, hi, hi_i) = match (flipped, op) {
                        (false, Lt) | (true, Gt) => (f64::NEG_INFINITY, true, f, false),
                        (false, Gt) | (true, Lt) => (f, false, f64::INFINITY, true),
                        (false, LtEq) | (true, GtEq) => (f64::NEG_INFINITY, true, f, true),
                        (false, GtEq) | (true, LtEq) => (f, true, f64::INFINITY, true),
                        _ => (f64::NEG_INFINITY, true, f64::INFINITY, true),
                    };
                    Some(Condition::RangeF64 {
                        column_id: cdef.id,
                        lo,
                        lo_inclusive: li,
                        hi,
                        hi_inclusive: hi_i,
                    })
                }
                _ => None,
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } if !negated => {
            let name = sp_ident_name(expr)?;
            let cdef = col_def(name)?;
            if is_int_ty(cdef.ty.clone()) {
                let lo = match sp_literal(low)? {
                    Value::Int64(n) => n,
                    _ => return None,
                };
                let hi = match sp_literal(high)? {
                    Value::Int64(n) => n,
                    _ => return None,
                };
                Some(Condition::Range {
                    column_id: cdef.id,
                    lo,
                    hi,
                })
            } else if is_float_ty(cdef.ty.clone()) {
                let lo = match sp_literal(low)? {
                    Value::Float64(f) => f,
                    _ => return None,
                };
                let hi = match sp_literal(high)? {
                    Value::Float64(f) => f,
                    _ => return None,
                };
                Some(Condition::RangeF64 {
                    column_id: cdef.id,
                    lo,
                    lo_inclusive: true,
                    hi,
                    hi_inclusive: true,
                })
            } else {
                None
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let name = sp_ident_name(expr)?;
            let cdef = col_def(name)?;
            if !has_bitmap(cdef.id) {
                return None;
            }
            let values: Vec<Vec<u8>> = list
                .iter()
                .map(|e| sp_literal(e).map(|v| v.encode_key()))
                .collect::<Option<_>>()?;
            (!values.is_empty()).then_some(Condition::BitmapIn {
                column_id: cdef.id,
                values,
            })
        }
        // `col IS NULL` / `col IS NOT NULL`. sqlparser 0.62 represents these as
        // `IsNull(expr)` / `IsNotNull(expr)` (and, defensively, `IsBoolean`).
        Expr::IsNull(inner) => {
            let cid = col_def(sp_ident_name(inner)?)?.id;
            Some(Condition::IsNull { column_id: cid })
        }
        Expr::IsNotNull(inner) => {
            let cid = col_def(sp_ident_name(inner)?)?.id;
            Some(Condition::IsNotNull { column_id: cid })
        }
        _ => None,
    }
}

/// Split a top-level AND tree into conjuncts, translating each to an exact
/// Condition. Returns `None` if any conjunct is inexact/unsupported.
fn translate_sqlparser_filter(
    expr: &sqlparser::ast::Expr,
    schema: &mongreldb_core::Schema,
) -> Option<Vec<mongreldb_core::Condition>> {
    use sqlparser::ast::{BinaryOperator, Expr};
    let mut out = Vec::new();
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                stack.push(left.as_ref());
                stack.push(right.as_ref());
            }
            other => out.push(translate_sqlparser_predicate(other, schema)?),
        }
    }
    Some(out)
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
    principal: Option<mongreldb_core::Principal>,
    principal_catalog_bound: bool,
    cache: ResultCache,
    /// Phase 16.5: logical-plan cache keyed by SQL string.
    plan_cache: parking_lot::Mutex<BoundedLru<String, datafusion::logical_expr::LogicalPlan>>,
    /// `table name → owning Table handle` for every registered table.
    tables: scored_sql::TableMap,
    /// Phase 17.3: named materialized views — `view name → defining SQL`.
    /// On `run("SELECT * FROM <view>")`, the defining SQL is executed (or the
    /// result-cache is hit). Invalidated automatically on commit (epoch bump).
    views: parking_lot::Mutex<HashMap<String, ViewDef>>,
    /// Databases attached via `ATTACH 'path' AS alias`, kept alive for the
    /// session's lifetime so their tables remain registered on the DataFusion
    /// context. Keyed by alias.
    attached_databases: parking_lot::Mutex<HashMap<String, Arc<Database>>>,
    /// SQL `BEGIN`/`COMMIT` staging for DML statements. Reads remain
    /// snapshot-at-scan; this batches SQL writes atomically when a client sends
    /// an explicit transaction block.
    sql_txn: parking_lot::Mutex<Option<Vec<commands::PendingSqlOp>>>,
    /// SAVEPOINT stack: `(name, staged-ops-length-at-savepoint)`. Truncated on
    /// `ROLLBACK TO name` and removed on `RELEASE name`.
    savepoints: parking_lot::Mutex<Vec<(String, usize)>>,
    /// Per-session state for SQL compatibility functions such as changes().
    sql_fn_state: Arc<extended_sql_functions::ExtendedSqlState>,
    /// Built-in plus app-provided external table modules available to this
    /// session.
    external_modules: Arc<ExternalModuleRegistry>,
    /// Names of `PREPARE`d statements tracked so they can be `DEALLOCATE`d when
    /// DDL invalidates the tables they reference (DataFusion's prepared-plan
    /// store is not cleared by the session's own result/plan caches).
    prepared_names: parking_lot::Mutex<std::collections::HashSet<String>>,
}

/// `(sql, snapshot_epoch) → cached result batches`.
type CacheKey = (String, u64);
type ResultCache = parking_lot::Mutex<BoundedLru<CacheKey, Arc<Vec<RecordBatch>>>>;

const SESSION_CACHE_MAX_ENTRIES: usize = 1024;

struct BoundedLru<K, V> {
    entries: HashMap<K, (V, u64)>,
    clock: u64,
    capacity: usize,
}

impl<K: Clone + Eq + Hash, V> BoundedLru<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            clock: 0,
            capacity: capacity.max(1),
        }
    }

    fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.clock = self.clock.wrapping_add(1);
        let (value, last_used) = self.entries.get_mut(key)?;
        *last_used = self.clock;
        Some(value)
    }

    fn insert(&mut self, key: K, value: V) {
        self.clock = self.clock.wrapping_add(1);
        if let Some(entry) = self.entries.get_mut(&key) {
            *entry = (value, self.clock);
            return;
        }
        if self.entries.len() >= self.capacity {
            // ponytail: scan 1024 entries on eviction; use a linked map if misses get hot.
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, last_used))| *last_used)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(key, (value, self.clock));
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.clock = 0;
    }
}

fn buffered_stream(batches: Vec<RecordBatch>) -> MongrelRecordBatchStream {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    let stream = futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, DataFusionError>));
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

impl MongrelSession {
    /// Create a session over a live `Table`. Takes ownership; wrap in `Arc` if you
    /// need to keep a handle for writes after registering the provider. Registers
    /// the `ann_search` UDF so SQL semantic-search predicates parse.
    pub fn new(db: Table) -> Self {
        let db = Arc::new(Mutex::new(db));
        let ctx = SessionContext::new();
        let sql_fn_state = Arc::new(extended_sql_functions::ExtendedSqlState::default());
        register_mongrel_functions(&ctx, Arc::clone(&sql_fn_state));
        let tables = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        scored_sql::register(&ctx, Arc::clone(&tables), None, None, false);
        let external_modules = Arc::new(ExternalModuleRegistry::default());
        Self {
            ctx,
            db: Some(db),
            database: None,
            principal: None,
            principal_catalog_bound: false,
            cache: parking_lot::Mutex::new(BoundedLru::new(SESSION_CACHE_MAX_ENTRIES)),
            plan_cache: parking_lot::Mutex::new(BoundedLru::new(SESSION_CACHE_MAX_ENTRIES)),
            tables,
            views: parking_lot::Mutex::new(HashMap::new()),
            attached_databases: parking_lot::Mutex::new(HashMap::new()),
            savepoints: parking_lot::Mutex::new(Vec::new()),
            sql_txn: parking_lot::Mutex::new(None),
            sql_fn_state,
            external_modules,
            prepared_names: parking_lot::Mutex::new(std::collections::HashSet::new()),
        }
    }

    pub fn new_with_external_modules(
        db: Table,
        modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    ) -> Result<Self> {
        let session = Self::new(db);
        for module in modules {
            session.register_external_module(module)?;
        }
        Ok(session)
    }

    /// Open a session over a multi-table [`Database`] (spec §12). Auto-registers
    /// every live table as a `MongrelProvider`; the cache epoch is driven by
    /// `Database::visible_epoch()` so any table's commit invalidates cached
    /// results.
    pub fn open(database: Arc<Database>) -> Result<Self> {
        Self::open_with_external_modules(database, std::iter::empty())
    }

    pub fn open_as(database: Arc<Database>, principal: mongreldb_core::Principal) -> Result<Self> {
        Self::open_with_external_modules_as(database, std::iter::empty(), Some(principal))
    }

    pub fn open_with_external_modules(
        database: Arc<Database>,
        modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    ) -> Result<Self> {
        Self::open_with_external_modules_as(database, modules, None)
    }

    pub fn open_with_external_modules_as(
        database: Arc<Database>,
        modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
        principal: Option<mongreldb_core::Principal>,
    ) -> Result<Self> {
        let ctx = SessionContext::new();
        let sql_fn_state = Arc::new(extended_sql_functions::ExtendedSqlState::default());
        register_mongrel_functions(&ctx, Arc::clone(&sql_fn_state));
        let external_modules = Arc::new(ExternalModuleRegistry::default());
        let principal_catalog_bound = principal
            .as_ref()
            .is_some_and(|principal| database.resolve_principal(&principal.username).is_some());
        for module in modules {
            external_modules.register(module)?;
        }

        let mut tables: HashMap<String, Arc<Mutex<Table>>> = HashMap::new();
        for name in database.table_names() {
            let handle = database.table(&name)?;
            let provider = MongrelProvider::new_secured(
                handle.clone(),
                Arc::clone(&database),
                name.clone(),
                principal.clone(),
            )?;
            ctx.register_table(&name, Arc::new(provider))
                .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
            tables.insert(name, handle);
        }
        for entry in database.external_tables() {
            let provider = external_modules.external_table_provider(&database, &entry)?;
            ctx.register_table(&entry.name, provider)
                .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        }

        // Pick a stable "primary" (lexicographically smallest name) for legacy
        // `db()` accessors. If the database is empty, `db()` returns `None`.
        let primary = {
            let mut names: Vec<&String> = tables.keys().collect();
            names.sort();
            names.first().and_then(|n| tables.get(*n).cloned())
        };
        let tables = Arc::new(parking_lot::Mutex::new(tables));
        scored_sql::register(
            &ctx,
            Arc::clone(&tables),
            Some(Arc::clone(&database)),
            principal.clone(),
            principal_catalog_bound,
        );

        Ok(Self {
            ctx,
            db: primary,
            database: Some(database),
            principal,
            principal_catalog_bound,
            cache: parking_lot::Mutex::new(BoundedLru::new(SESSION_CACHE_MAX_ENTRIES)),
            plan_cache: parking_lot::Mutex::new(BoundedLru::new(SESSION_CACHE_MAX_ENTRIES)),
            tables,
            views: parking_lot::Mutex::new(HashMap::new()),
            attached_databases: parking_lot::Mutex::new(HashMap::new()),
            savepoints: parking_lot::Mutex::new(Vec::new()),
            sql_txn: parking_lot::Mutex::new(None),
            sql_fn_state,
            external_modules,
            prepared_names: parking_lot::Mutex::new(std::collections::HashSet::new()),
        })
    }

    pub fn principal(&self) -> Option<mongreldb_core::Principal> {
        self.principal.as_ref().and_then(|principal| {
            if self.principal_catalog_bound {
                self.database
                    .as_ref()
                    .and_then(|database| database.resolve_principal(&principal.username))
            } else {
                Some(principal.clone())
            }
        })
    }

    fn security_context_active(&self) -> bool {
        self.principal.is_some()
            || self.database.as_ref().is_some_and(|database| {
                database.principal_snapshot().is_some() || {
                    let security = database.security_catalog();
                    !security.rls_tables.is_empty()
                        || !security.policies.is_empty()
                        || !security.masks.is_empty()
                }
            })
    }

    pub fn register_external_module(&self, module: Arc<dyn ExternalTableModule>) -> Result<()> {
        self.external_modules.register(module)?;
        self.clear_cache();
        Ok(())
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
        self.create_view_with_schema(name, sql, CoreSchema::default(), HashMap::new());
    }

    pub(crate) fn create_view_with_schema(
        &self,
        name: &str,
        sql: &str,
        schema: CoreSchema,
        input_types: HashMap<u16, Option<TypeId>>,
    ) {
        self.views.lock().insert(
            name.to_string(),
            ViewDef {
                sql: sql.to_string(),
                schema,
                input_types,
            },
        );
    }

    /// Drop a named materialized view.
    pub fn drop_view(&self, name: &str) {
        self.views.lock().remove(name);
    }

    pub(crate) fn view_schema(&self, name: &str) -> Option<CoreSchema> {
        self.views.lock().get(name).map(|view| view.schema.clone())
    }

    pub(crate) fn view_definition(&self, name: &str) -> Option<ViewDef> {
        self.views.lock().get(name).cloned()
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
        let provider = MongrelProvider::new_secured(
            handle.clone(),
            Arc::clone(db),
            name.to_string(),
            self.principal.clone(),
        )?;
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
    /// Intercept `SELECT ... FROM information_schema.tables` and return
    /// a synthesized batch listing tables, views, and triggers. Returns
    /// `None` if the SQL doesn't reference that name.
    fn try_catalog_introspection(&self, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
        let lower = sql.to_ascii_lowercase();
        if !lower.contains("information_schema.tables") {
            return Ok(None);
        }
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let mut types: Vec<String> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut tbl_names: Vec<String> = Vec::new();

        // Tables.
        let principal = self.principal();
        let can_see = |name: &str| match &self.database {
            Some(database) => database
                .select_column_ids_for(name, principal.as_ref())
                .is_ok(),
            None => true,
        };
        for name in self.tables.lock().keys().filter(|name| can_see(name)) {
            types.push("table".into());
            names.push(name.clone());
            tbl_names.push(name.clone());
        }
        // Views (session-scoped).
        for name in self.views.lock().keys() {
            types.push("view".into());
            names.push(name.clone());
            tbl_names.push(name.clone());
        }
        // Triggers (engine-side, if a Database is attached).
        if let Some(db) = &self.database {
            for t in db.triggers() {
                let target_name = match &t.target {
                    mongreldb_core::trigger::TriggerTarget::Table(n)
                    | mongreldb_core::trigger::TriggerTarget::View(n) => n.clone(),
                };
                if !can_see(&target_name) {
                    continue;
                }
                types.push("trigger".into());
                names.push(t.name.clone());
                tbl_names.push(target_name);
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("type", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("tbl_name", DataType::Utf8, false),
            Field::new("rootpage", DataType::Int64, false),
            Field::new("sql", DataType::Utf8, true),
        ]));
        let n = names.len();
        let rootpages: Vec<i64> = vec![0; n];
        let sqls: Vec<Option<&str>> = vec![None; n];
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(types)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
                Arc::new(StringArray::from(tbl_names)) as ArrayRef,
                Arc::new(Int64Array::from(rootpages)) as ArrayRef,
                Arc::new(StringArray::from(sqls)) as ArrayRef,
            ],
        )
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
        Ok(Some(vec![batch]))
    }

    /// §5.3 direct SQL dispatch: recognize a simple single-table `SELECT` from
    /// the raw SQL via the vendored `sqlparser` AST and serve it straight from
    /// the native column cursor, **bypassing DataFusion parse+plan+optimize**.
    /// Returns `Ok(None)` (→ fall through to `ctx.sql()`) for any shape it
    /// cannot serve *exactly*, or on any parse error.
    fn try_direct_dispatch(&self, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
        if self.security_context_active() {
            return Ok(None);
        }

        use arrow::array::ArrayRef;
        use mongreldb_core::Condition;
        use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor};
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        // Any parse error, or more than one statement → fall through.
        let Ok(stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
            return Ok(None);
        };
        if stmts.len() != 1 {
            return Ok(None);
        }
        let Statement::Query(query) = stmts.into_iter().next().unwrap() else {
            return Ok(None);
        };
        let Query { body, .. } = *query;
        let select = match *body {
            SetExpr::Select(s) => *s,
            _ => return Ok(None),
        };
        // v1: fall through if LIMIT/OFFSET is present (can't read the fields
        // portably; a conservative token check keeps correctness safe).
        let lower_sql = sql.to_lowercase();
        if lower_sql.contains(" limit ") || lower_sql.contains(" offset ") {
            return Ok(None);
        }
        // Reject shapes we don't handle: DISTINCT / GROUP BY / HAVING / multi-FROM / joins.
        use sqlparser::ast::GroupByExpr;
        if select.distinct.is_some()
            || !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty())
            || select.having.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
        {
            return Ok(None);
        }
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => return Ok(None),
        };
        let Some(table_name) = table_name else {
            return Ok(None);
        };

        // v1 only dispatches FILTERED single-table SELECTs. An unfiltered `SELECT
        // *`/`SELECT cols` already streams efficiently through the scan path
        // (with ≤65 536-row batch chunking + Arrow shadow writes), which the
        // direct path's single-shot column decode can't preserve — so leave it
        // to DataFusion. The win here is the cold filtered-SELECT planning cost.
        if select.selection.is_none() {
            return Ok(None);
        }

        // Projection: only `*` or a list of bare column identifiers.
        let mut proj_names: Option<Vec<String>> = None;
        for item in &select.projection {
            match item {
                SelectItem::Wildcard(_) => {}
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    proj_names
                        .get_or_insert_with(Vec::new)
                        .push(ident.value.clone());
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                    if let Some(last) = idents.last() {
                        proj_names
                            .get_or_insert_with(Vec::new)
                            .push(last.value.clone());
                    }
                }
                _ => return Ok(None),
            }
        }

        // Resolve the table handle.
        let handle = match self.tables.lock().get(&table_name).cloned() {
            Some(h) => h,
            None => return Ok(None),
        };

        // SQL SELECT → require Select permission on the target table.
        if let Some(db) = &self.database {
            db.require_table(
                &table_name,
                mongreldb_core::auth_state::RequiredPermission::Select,
            )?;
        }

        let mut db = handle.lock();
        let schema = db.schema().clone();
        // Translate WHERE against the live schema; an inexact/unsupported
        // predicate → fall through to DataFusion (which re-applies residuals).
        let conditions: Vec<Condition> = match &select.selection {
            Some(expr) => match translate_sqlparser_filter(expr, &schema) {
                Some(c) => c,
                None => return Ok(None),
            },
            None => Vec::new(),
        };
        if !conditions.is_empty() && db.ensure_indexes_complete().is_err() {
            return Ok(None);
        }
        let snap = db.snapshot();

        // Resolve projected column ids + Arrow field list (in projection order).
        let mut col_ids: Vec<u16> = Vec::new();
        let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
        let resolve_col = |name: &str| -> Option<&mongreldb_core::schema::ColumnDef> {
            schema.columns.iter().find(|c| c.name == name)
        };
        match &proj_names {
            None => {
                for c in &schema.columns {
                    col_ids.push(c.id);
                    fields.push(arrow::datatypes::Field::new(
                        &c.name,
                        arrow_conv::arrow_data_type(&c.ty)?,
                        c.flags.contains(mongreldb_core::ColumnFlags::NULLABLE),
                    ));
                }
            }
            Some(names) => {
                for n in names {
                    let cdef = match resolve_col(n) {
                        Some(c) => c,
                        None => return Ok(None), // unknown column → let DataFusion error
                    };
                    col_ids.push(cdef.id);
                    fields.push(arrow::datatypes::Field::new(
                        &cdef.name,
                        arrow_conv::arrow_data_type(&cdef.ty)?,
                        cdef.flags.contains(mongreldb_core::ColumnFlags::NULLABLE),
                    ));
                }
            }
        }

        // Execute via the same native column path MongrelProvider::scan uses.
        let cols = if !conditions.is_empty() {
            match db.query_columns_native_cached(&conditions, Some(&col_ids), snap) {
                Ok(Some(c)) => c,
                Ok(None) => db
                    .visible_columns_native(snap, Some(&col_ids))
                    .map_err(MongrelQueryError::Core)?,
                Err(_) => return Ok(None),
            }
        } else {
            db.visible_columns_native(snap, Some(&col_ids))
                .map_err(MongrelQueryError::Core)?
        };
        drop(db);

        // Order decoded columns into projection order, then build one batch.
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_ids.len());
        for cid in &col_ids {
            let col = cols
                .iter()
                .find(|(id, _)| id == cid)
                .map(|(_, c)| c.clone());
            let Some(col) = col else { return Ok(None) };
            let ty = schema
                .columns
                .iter()
                .find(|c| c.id == *cid)
                .map(|c| c.ty.clone())
                .unwrap_or(mongreldb_core::schema::TypeId::Int64);
            arrays.push(arrow_conv::native_to_array(ty, &col)?);
        }
        let batch_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(batch_schema, arrays)
            .map_err(|e| MongrelQueryError::Arrow(format!("direct dispatch batch build: {e}")))?;

        mongreldb_core::trace::QueryTrace::record(|t| {
            t.scan_mode = mongreldb_core::trace::ScanMode::DirectDispatch;
            t.planning_nanos = 0; // we bypassed DataFusion planning
        });
        Ok(Some(vec![batch]))
    }

    async fn dataframe(&self, sql: &str) -> Result<datafusion::dataframe::DataFrame> {
        // Prepared commands have their own DataFusion store. Caching EXECUTE
        // would create one entry per parameter set.
        let use_plan_cache = !is_prepared_stmt_sql(sql);
        if let Some(plan) = use_plan_cache
            .then(|| self.plan_cache.lock().get(sql).cloned())
            .flatten()
        {
            return Ok(datafusion::dataframe::DataFrame::new(
                self.ctx.state(),
                plan,
            ));
        }

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        if use_plan_cache {
            self.plan_cache
                .lock()
                .insert(sql.to_string(), df.logical_plan().clone());
        }
        Ok(df)
    }

    fn prepare_as_of_query(&self, sql: &str) -> Result<Option<AsOfQuery>> {
        static AS_OF_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let re = AS_OF_RE.get_or_init(|| {
            regex::Regex::new(
                r#"(?i)\b(FROM|JOIN)\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+AS\s+OF\s+EPOCH\s+([0-9]+)\b"#,
            )
            .expect("valid AS OF regex")
        });
        let mut captures = re.captures_iter(sql);
        let Some(capture) = captures.next() else {
            return Ok(None);
        };
        if captures.next().is_some() {
            return Err(MongrelQueryError::Schema(
                "AS OF EPOCH currently supports one historical table per query".into(),
            ));
        }
        let database = self.database.as_ref().ok_or_else(|| {
            MongrelQueryError::Schema("AS OF EPOCH requires a Database-backed session".into())
        })?;
        let whole = capture.get(0).expect("whole AS OF match");
        let keyword = capture.get(1).expect("FROM/JOIN capture").as_str();
        let table_token = capture.get(2).expect("table capture").as_str();
        let table_name = table_token.trim_matches('"');
        let epoch = capture
            .get(3)
            .expect("epoch capture")
            .as_str()
            .parse::<u64>()
            .map_err(|error| MongrelQueryError::Schema(format!("AS OF epoch: {error}")))?;
        let temp_id = NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp_name = format!("__mongrel_as_of_{temp_id}");

        // Preserve an explicit alias after the epoch. Without one, alias the
        // temporary provider back to the original table name so qualifiers
        // such as `events.id` keep working.
        let mut replace_end = whole.end();
        let mut alias = table_token.to_string();
        let tail = &sql[replace_end..];
        let whitespace = tail.len() - tail.trim_start().len();
        let trimmed_tail = tail.trim_start();
        if trimmed_tail
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("as "))
        {
            let alias_text = &trimmed_tail[3..];
            let alias_len = alias_text
                .find(|character: char| {
                    character.is_ascii_whitespace() || character == ',' || character == ';'
                })
                .unwrap_or(alias_text.len());
            if alias_len == 0 {
                return Err(MongrelQueryError::Schema(
                    "AS OF EPOCH AS requires an alias".into(),
                ));
            }
            alias = alias_text[..alias_len].to_string();
            replace_end += whitespace + 3 + alias_len;
        } else {
            let alias_len = trimmed_tail
                .find(|character: char| {
                    character.is_ascii_whitespace() || character == ',' || character == ';'
                })
                .unwrap_or(trimmed_tail.len());
            let candidate = &trimmed_tail[..alias_len];
            let keyword = candidate.to_ascii_lowercase();
            let clause_keyword = matches!(
                keyword.as_str(),
                "where"
                    | "join"
                    | "left"
                    | "right"
                    | "inner"
                    | "outer"
                    | "cross"
                    | "full"
                    | "on"
                    | "group"
                    | "order"
                    | "having"
                    | "limit"
                    | "offset"
                    | "union"
                    | "intersect"
                    | "except"
            );
            let valid_alias = candidate
                .as_bytes()
                .first()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_' || *byte == b'"');
            if alias_len > 0 && valid_alias && !clause_keyword {
                alias = candidate.to_string();
                replace_end += whitespace + alias_len;
            }
        }
        let replacement = format!("{keyword} {temp_name} AS {alias}");
        let mut rewritten = String::with_capacity(sql.len() + replacement.len());
        rewritten.push_str(&sql[..whole.start()]);
        rewritten.push_str(&replacement);
        rewritten.push_str(&sql[replace_end..]);

        let (snapshot, retention) = database.snapshot_at_owned(mongreldb_core::Epoch(epoch))?;
        let handle = database.table(table_name)?;
        let provider = MongrelProvider::new_historical(
            handle,
            snapshot,
            retention,
            Some(ProviderSecurity {
                database: Arc::clone(database),
                table: table_name.to_string(),
                principal: self.principal.clone(),
                principal_catalog_bound: self.principal_catalog_bound,
            }),
        )?;
        self.ctx
            .register_table(&temp_name, Arc::new(provider))
            .map_err(|error| MongrelQueryError::DataFusion(error.to_string()))?;
        Ok(Some(AsOfQuery {
            sql: rewritten,
            registration: AsOfRegistration {
                ctx: self.ctx.clone(),
                table_name: temp_name,
            },
        }))
    }

    async fn run_as_of(&self, query: AsOfQuery) -> Result<Vec<RecordBatch>> {
        let AsOfQuery { sql, registration } = query;
        let _registration = registration;
        let plan_start = std::time::Instant::now();
        let dataframe = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|error| MongrelQueryError::DataFusion(error.to_string()))?;
        mongreldb_core::trace::QueryTrace::record(|trace| {
            trace.planning_nanos = plan_start.elapsed().as_nanos() as u64;
        });
        dataframe
            .collect()
            .await
            .map_err(|error| MongrelQueryError::DataFusion(error.to_string()))
    }

    async fn run_as_of_stream(&self, query: AsOfQuery) -> Result<MongrelRecordBatchStream> {
        use futures::StreamExt;

        let AsOfQuery { sql, registration } = query;
        let dataframe = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|error| MongrelQueryError::DataFusion(error.to_string()))?;
        let stream = dataframe
            .execute_stream()
            .await
            .map_err(|error| MongrelQueryError::DataFusion(error.to_string()))?;
        let schema = stream.schema();
        let guarded = futures::stream::unfold(
            (stream, registration),
            |(mut stream, registration)| async move {
                stream
                    .next()
                    .await
                    .map(|item| (item, (stream, registration)))
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, guarded)))
    }

    /// Execute SQL as a DataFusion record-batch stream. Query results bypass
    /// the result cache and native batch-producing fast paths. Commands and
    /// MongrelDB's recursive-CTE compatibility path remain buffered because
    /// they do not produce a DataFusion stream.
    pub async fn run_stream(&self, sql: &str) -> Result<MongrelRecordBatchStream> {
        if strip_explain_query_plan(sql).is_some() {
            return self.run(sql).await.map(buffered_stream);
        }

        let trimmed = sql.trim();
        if trimmed.contains(';') && !is_trigger_body(trimmed) {
            let stmts = split_sql_statements(trimmed);
            let non_empty: Vec<&str> = stmts
                .iter()
                .map(String::as_str)
                .filter(|stmt| !stmt.trim().is_empty() && stmt.trim() != ";")
                .collect();
            if non_empty.is_empty() {
                return Ok(buffered_stream(Vec::new()));
            }
            if stmts.len() > 1 {
                for stmt in &non_empty[..non_empty.len() - 1] {
                    self.run(stmt).await?;
                }
                return Box::pin(self.run_stream(non_empty[non_empty.len() - 1])).await;
            }
        }

        if let Some(batches) = commands::try_run_command(self, sql).await? {
            return Ok(buffered_stream(batches));
        }

        if let Some(query) = self.prepare_as_of_query(sql)? {
            return self.run_as_of_stream(query).await;
        }

        let resolved = self.resolve_view_sql(sql);
        let resolved = self.rewrite_external_module_compat_sql(&resolved);
        let resolved = rewrite_compat_function_calls(&resolved);
        let effective_sql = normalize_sql(&resolved);
        let sql = effective_sql.as_str();

        if let Some(batches) = self.try_catalog_introspection(sql)? {
            return Ok(buffered_stream(batches));
        }

        let plan_start = std::time::Instant::now();
        let external_module_scan = self.query_references_external_module(sql);
        let df = self.dataframe(sql).await?;
        self.track_prepared_name(sql);
        mongreldb_core::trace::QueryTrace::record(|trace| {
            trace.planning_nanos = plan_start.elapsed().as_nanos() as u64;
        });
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        if external_module_scan {
            mongreldb_core::trace::QueryTrace::record(|trace| {
                trace.scan_mode = mongreldb_core::trace::ScanMode::ExternalModule;
            });
        }
        Ok(stream)
    }

    /// Run a SQL statement: DDL/commands are intercepted; otherwise a result
    /// cache keyed by `(normalized SQL, snapshot epoch)` memoizes batches.
    /// §5.3: simple single-table SELECTs are served by [`try_direct_dispatch`]
    /// (no DataFusion planning) before falling back to the full DataFusion path.
    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        if let Some(inner) = strip_explain_query_plan(sql) {
            return self.explain_query_plan(inner).await;
        }

        // Multi-statement support: if the SQL contains semicolons, first try
        // to split into individual statements. This must happen BEFORE command
        // dispatch because `try_run_command` would fail on multi-statement SQL.
        // But CREATE TRIGGER bodies contain semicolons inside BEGIN...END, so
        // we only split if the first semicolon is NOT inside a BEGIN...END block.
        let trimmed = sql.trim();
        if trimmed.contains(';') && !is_trigger_body(trimmed) {
            let stmts = split_sql_statements(trimmed);
            // If all statements are empty (e.g. ";;;"), return empty result.
            let non_empty: Vec<&String> = stmts
                .iter()
                .filter(|s| !s.trim().is_empty() && s.trim() != ";")
                .collect();
            if non_empty.is_empty() {
                return Ok(Vec::new());
            }
            if stmts.len() > 1 {
                let mut last = Vec::new();
                for stmt in &stmts {
                    let stmt = stmt.trim();
                    if stmt.is_empty() {
                        continue;
                    }
                    last = Box::pin(self.run(stmt)).await?;
                }
                return Ok(last);
            }
        }

        // Try command dispatch (handles DDL/DML/triggers/views/etc.).
        if let Some(batches) = commands::try_run_command(self, sql).await? {
            return Ok(batches);
        }

        // Multi-statement support: if the SQL wasn't handled as a single
        // command and contains semicolons, split into individual statements
        // and execute each sequentially. This catches `SELECT 1; SELECT 2`
        // style multi-statement queries. Commands with semicolons in their
        // bodies (CREATE TRIGGER ... BEGIN ... END;) are handled above.
        let trimmed = sql.trim();
        if trimmed.contains(';') {
            let stmts = split_sql_statements(trimmed);
            if stmts.len() > 1 {
                let mut last = Vec::new();
                for stmt in &stmts {
                    let stmt = stmt.trim();
                    if stmt.is_empty() {
                        continue;
                    }
                    last = Box::pin(self.run(stmt)).await?;
                }
                return Ok(last);
            }
        }
        // P4.2: intercept DDL when a Database is attached.
        let lower = sql.trim_start().to_lowercase();
        if lower.starts_with("create table") {
            if let Some(db) = &self.database {
                db.require_for(self.principal().as_ref(), &mongreldb_core::Permission::Ddl)?;
                let (name, schema) = parse_create_table(sql)?;
                db.create_table(&name, schema)?;
                let handle = db.table(&name)?;
                let provider = MongrelProvider::new_secured(
                    handle.clone(),
                    Arc::clone(db),
                    name.clone(),
                    self.principal.clone(),
                )?;
                self.ctx
                    .register_table(&name, Arc::new(provider))
                    .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                self.tables.lock().insert(name, handle);
                self.invalidate_prepared_statements().await;
                self.clear_cache();
                return Ok(Vec::new());
            }
        }
        if lower.starts_with("drop table") {
            if let Some(db) = &self.database {
                db.require_for(self.principal().as_ref(), &mongreldb_core::Permission::Ddl)?;
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
                self.invalidate_prepared_statements().await;
                self.clear_cache();
                return Ok(Vec::new());
            }
        }
        if lower.starts_with("alter table") {
            if let Some(db) = &self.database {
                db.require_for(self.principal().as_ref(), &mongreldb_core::Permission::Ddl)?;
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
                        let provider = MongrelProvider::new_secured(
                            handle.clone(),
                            Arc::clone(db),
                            new_name.clone(),
                            self.principal.clone(),
                        )?;
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
                self.invalidate_prepared_statements().await;
                self.clear_cache();
                return Ok(Vec::new());
            }
        }

        if let Some(query) = self.prepare_as_of_query(sql)? {
            return self.run_as_of(query).await;
        }

        // Phase 17.3: intercept `SELECT ... FROM <view_name>` and rewrite to
        // the view's defining SQL.
        let resolved = self.resolve_view_sql(sql);
        let resolved = self.rewrite_external_module_compat_sql(&resolved);
        let resolved = rewrite_compat_function_calls(&resolved);
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
        let has_ttl_table = self
            .tables
            .lock()
            .values()
            .any(|table| table.lock().ttl().is_some());
        let result_cacheable = !extended_sql_functions::contains_volatile_extended_function(sql)
            && !is_explain_analyze(sql)
            && !is_prepared_stmt_sql(sql)
            && !has_ttl_table;
        if result_cacheable {
            if let Some(hit) = self.cache.lock().get(&key) {
                return Ok((**hit).clone());
            }
        }
        // information_schema.tables: intercept catalog-introspection SELECTs
        // and synthesize a result batch.
        if let Some(batches) = self.try_catalog_introspection(sql)? {
            if result_cacheable {
                self.cache.lock().insert(key, Arc::new(batches.clone()));
            }
            return Ok(batches);
        }
        // §5.3: direct SQL dispatch for simple single-table SELECTs — bypasses
        // DataFusion parse+plan+optimize. Served batches are memoized into the
        // result cache like the normal path. Returns None (→ fall through) for
        // any shape it cannot serve exactly.
        if let Some(batches) = self.try_direct_dispatch(sql)? {
            if result_cacheable {
                self.cache.lock().insert(key, Arc::new(batches.clone()));
            }
            return Ok(batches);
        }
        // Phase 16.5: check the logical-plan cache before re-parsing.
        let plan_start = std::time::Instant::now();
        let external_module_scan = self.query_references_external_module(sql);
        let df = self.dataframe(sql).await?;
        // Track prepared-statement names so DDL can DEALLOCATE them (prevents a
        // prepared plan from querying a detached/old table after DROP+recreate).
        self.track_prepared_name(sql);
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
        if external_module_scan {
            mongreldb_core::trace::QueryTrace::record(|t| {
                t.scan_mode = mongreldb_core::trace::ScanMode::ExternalModule;
            });
        }
        if result_cacheable {
            self.cache.lock().insert(key, Arc::new(batches.clone()));
        }
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

    /// Record/remove a prepared-statement name from the tracking set. Called
    /// after the DataFusion execution path runs a `PREPARE`/`DEALLOCATE` so the
    /// session knows which names to invalidate when DDL changes the schema.
    fn track_prepared_name(&self, sql: &str) {
        let lower = sql.trim_start().to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("prepare ") {
            if let Some(name) = parse_stmt_ident(rest) {
                self.prepared_names.lock().insert(name);
            }
        } else if let Some(rest) = lower.strip_prefix("deallocate ") {
            if let Some(name) = parse_stmt_ident(rest) {
                self.prepared_names.lock().remove(&name);
            }
        }
    }

    /// `DEALLOCATE` every tracked prepared statement. Called on table DDL so a
    /// prepared plan cannot keep querying a dropped/altered/recreated table
    /// (DataFusion's prepared-plan store is otherwise untouched by `clear_cache`
    /// and would retain a plan bound to the old table provider). Errors from an
    /// unknown name are ignored (the plan was never stored, e.g. a failed
    /// PREPARE whose name was optimistically tracked).
    pub async fn invalidate_prepared_statements(&self) {
        let names: Vec<String> = self.prepared_names.lock().drain().collect();
        for name in names {
            // `DEALLOCATE <name>` — no params, safe (name was validated at
            // PREPARE time on the server path; here it came from our own
            // tracking so it is a bare identifier).
            let sql = format!("DEALLOCATE {name}");
            if self.ctx.sql(&sql).await.is_err() {
                // Unknown/stale name — ignore; the goal is just to drop it.
            }
        }
    }

    async fn explain_query_plan(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let explain_sql = format!("EXPLAIN {}", sql.trim().trim_end_matches(';'));
        let batches = self
            .ctx
            .sql(&explain_sql)
            .await
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?
            .collect()
            .await
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        let mut detail = self.mongrel_query_plan_details(sql);
        for batch in &batches {
            if batch.num_columns() < 2 {
                continue;
            }
            let Some(plan_type) = batch.column(0).as_any().downcast_ref::<StringArray>() else {
                continue;
            };
            let Some(plan) = batch.column(1).as_any().downcast_ref::<StringArray>() else {
                continue;
            };
            for row in 0..batch.num_rows() {
                let prefix = plan_type.value(row);
                for line in plan.value(row).lines() {
                    let line = line.trim();
                    if !line.is_empty() {
                        detail.push(format!("DATAFUSION {prefix}: {line}"));
                    }
                }
            }
        }
        if detail.is_empty() {
            detail.push("plan unavailable".to_string());
        }
        let ids = (0..detail.len()).map(|i| i as i64).collect::<Vec<_>>();
        let parents = vec![0_i64; detail.len()];
        let notused = vec![0_i64; detail.len()];
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("parent", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("notused", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("detail", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(Int64Array::from(parents)),
                Arc::new(Int64Array::from(notused)),
                Arc::new(StringArray::from(detail)),
            ],
        )
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
        Ok(vec![batch])
    }

    fn mongrel_query_plan_details(&self, sql: &str) -> Vec<String> {
        use sqlparser::ast::{GroupByExpr, OrderByKind, SetExpr, Statement};
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let Ok(stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
            return Vec::new();
        };
        let Some(Statement::Query(query)) = stmts.first() else {
            return Vec::new();
        };

        fn collect(session: &MongrelSession, query: &sqlparser::ast::Query, out: &mut Vec<String>) {
            use sqlparser::ast::{SetOperator, TableWithJoins};
            match query.body.as_ref() {
                SetExpr::Select(select) => {
                    for TableWithJoins { relation, joins } in &select.from {
                        session.push_table_plan(relation, select.selection.as_ref(), out);
                        for join in joins {
                            session.push_table_plan(&join.relation, None, out);
                        }
                    }
                    if select.distinct.is_some() {
                        out.push("USE TEMP B-TREE FOR DISTINCT".to_string());
                    }
                    let grouped = match &select.group_by {
                        GroupByExpr::All(_) => true,
                        GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
                    };
                    if grouped {
                        out.push("USE TEMP B-TREE FOR GROUP BY".to_string());
                    }
                    let ordered = query.order_by.as_ref().is_some_and(|order_by| {
                        matches!(order_by.kind, OrderByKind::All(_))
                            || matches!(&order_by.kind, OrderByKind::Expressions(exprs) if !exprs.is_empty())
                    });
                    if ordered {
                        out.push("USE TEMP B-TREE FOR ORDER BY".to_string());
                    }
                }
                SetExpr::Query(query) => collect(session, query, out),
                SetExpr::SetOperation {
                    left, op, right, ..
                } => {
                    let label = match op {
                        SetOperator::Union => "COMPOUND QUERY UNION",
                        SetOperator::Except => "COMPOUND QUERY EXCEPT",
                        SetOperator::Intersect => "COMPOUND QUERY INTERSECT",
                        _ => "COMPOUND QUERY",
                    };
                    out.push(label.to_string());
                    collect_set_expr(session, left, out);
                    collect_set_expr(session, right, out);
                }
                _ => {}
            }
        }

        fn collect_set_expr(session: &MongrelSession, expr: &SetExpr, out: &mut Vec<String>) {
            match expr {
                SetExpr::Select(select) => {
                    for table in &select.from {
                        session.push_table_plan(&table.relation, select.selection.as_ref(), out);
                    }
                }
                SetExpr::Query(query) => collect(session, query, out),
                SetExpr::SetOperation { left, right, .. } => {
                    collect_set_expr(session, left, out);
                    collect_set_expr(session, right, out);
                }
                _ => {}
            }
        }

        let mut out = Vec::new();
        collect(self, query, &mut out);
        out
    }

    fn push_table_plan(
        &self,
        relation: &sqlparser::ast::TableFactor,
        selection: Option<&sqlparser::ast::Expr>,
        out: &mut Vec<String>,
    ) {
        let sqlparser::ast::TableFactor::Table { name, alias, .. } = relation else {
            out.push("SCAN SUBQUERY".to_string());
            return;
        };
        let table_name = name.to_string();
        let display_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table_name.clone());
        let Some(handle) = self.tables.lock().get(&table_name).cloned() else {
            out.push(format!("SCAN {display_name}"));
            return;
        };
        let schema = handle.lock().schema().clone();
        let searchable = selection
            .and_then(|expr| translate_sqlparser_filter(expr, &schema))
            .is_some_and(|conditions| !conditions.is_empty());
        if searchable {
            out.push(format!("SEARCH {display_name} USING MONGREL INDEX"));
        } else {
            out.push(format!("SCAN {display_name}"));
        }
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
        for (name, view) in views.iter() {
            result = replace_from_view(&result, name, &view.sql);
        }
        result
    }

    fn rewrite_external_module_compat_sql(&self, sql: &str) -> String {
        let Some(db) = &self.database else {
            return sql.to_string();
        };
        rewrite_fts_match_compat_sql(sql, db)
    }

    fn query_references_external_module(&self, sql: &str) -> bool {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .ok()
            .is_some_and(|statements| {
                statements.iter().any(|statement| match statement {
                    Statement::Query(query) => self.query_uses_external_module(query),
                    _ => false,
                })
            })
    }

    fn query_uses_external_module(&self, query: &sqlparser::ast::Query) -> bool {
        self.set_expr_uses_external_module(query.body.as_ref())
    }

    fn set_expr_uses_external_module(&self, expr: &sqlparser::ast::SetExpr) -> bool {
        use sqlparser::ast::SetExpr;

        match expr {
            SetExpr::Select(select) => select
                .from
                .iter()
                .any(|table| self.table_with_joins_uses_external_module(table)),
            SetExpr::Query(query) => self.query_uses_external_module(query),
            SetExpr::SetOperation { left, right, .. } => {
                self.set_expr_uses_external_module(left)
                    || self.set_expr_uses_external_module(right)
            }
            _ => false,
        }
    }

    fn table_with_joins_uses_external_module(
        &self,
        table: &sqlparser::ast::TableWithJoins,
    ) -> bool {
        self.table_factor_uses_external_module(&table.relation)
            || table
                .joins
                .iter()
                .any(|join| self.table_factor_uses_external_module(&join.relation))
    }

    fn table_factor_uses_external_module(&self, relation: &sqlparser::ast::TableFactor) -> bool {
        use sqlparser::ast::{Expr, TableFactor};

        match relation {
            TableFactor::Table { name, args, .. } => {
                let table_name = name.to_string();
                self.database
                    .as_ref()
                    .is_some_and(|db| db.external_table(&table_name).is_some())
                    || (args.is_some() && self.external_modules.contains(&table_name))
            }
            TableFactor::Function { name, .. } => self.external_modules.contains(&name.to_string()),
            TableFactor::TableFunction {
                expr: Expr::Function(func),
                ..
            } => self.external_modules.contains(&func.name.to_string()),
            TableFactor::Derived { subquery, .. } => self.query_uses_external_module(subquery),
            _ => false,
        }
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
        if self.security_context_active() {
            return Ok(None);
        }
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
        if self.security_context_active() {
            return Ok(None);
        }
        let tables = self.tables.lock();
        fk_join::try_fk_join(&tables, plan)
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Register a custom scalar SQL function on this session.
    ///
    /// This is the Rust escape hatch for application-defined SQL functions. The
    /// session's plan and result caches are cleared because function resolution
    /// can change query output without advancing the storage epoch.
    pub fn register_scalar_udf(&self, f: ScalarUDF) {
        self.ctx.register_udf(f);
        self.clear_cache();
    }

    /// Register a custom aggregate SQL function on this session.
    pub fn register_aggregate_udf(&self, f: AggregateUDF) {
        self.ctx.register_udaf(f);
        self.clear_cache();
    }

    /// Register a custom window SQL function on this session.
    pub fn register_window_udf(&self, f: WindowUDF) {
        self.ctx.register_udwf(f);
        self.clear_cache();
    }
}

fn register_mongrel_functions(
    ctx: &SessionContext,
    sql_fn_state: Arc<extended_sql_functions::ExtendedSqlState>,
) {
    ctx.register_udf(ScalarUDF::from(udf::AnnSearchUdf::new()));
    ctx.register_udf(ScalarUDF::from(udf::SparseMatchUdf::new()));
    ctx.register_udf(ScalarUDF::from(udf::RTreeIntersectsUdf::new()));
    ctx.register_udf(ScalarUDF::from(udf::FtsRankUdf::new()));
    for udaf in percentile::percentile_udafs() {
        ctx.register_udaf(udaf);
    }
    extended_sql_functions::register_extended_sql_functions_with_state(ctx, sql_fn_state);
}

/// Check if the SQL is a CREATE TRIGGER statement with a BEGIN...END body.
/// These contain semicolons inside the body that must NOT be split.
fn is_trigger_body(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("create trigger") && !lower.contains("create or replace trigger") {
        return false;
    }
    lower.contains("begin") && lower.contains("end")
}

/// Split a SQL string into individual statements on semicolons, respecting
/// single-quoted strings (`'...'`), double-quoted identifiers (`"..."`),
/// dollar-quoting (`$$...$$` or `$tag$...$tag$`), and line/block comments.
/// A trailing semicolon is not an empty statement.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    while i < n {
        let c = b[i];
        // Line comment: skip to end of line.
        if c == b'-' && i + 1 < n && b[i + 1] == b'-' {
            while i < n && b[i] != b'\n' {
                current.push(c as char);
                i += 1;
            }
            continue;
        }
        // Block comment: skip to matching close.
        if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
            current.push_str("/*");
            i += 2;
            let mut depth = 1;
            while i + 1 < n && depth > 0 {
                if b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    current.push_str("/*");
                    i += 2;
                } else if b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    current.push_str("*/");
                    i += 2;
                } else {
                    current.push(b[i] as char);
                    i += 1;
                }
            }
            continue;
        }
        // Single-quoted string.
        if c == b'\'' {
            current.push('\'');
            i += 1;
            while i < n {
                current.push(b[i] as char);
                if b[i] == b'\'' {
                    // Check for doubled quote escape.
                    if i + 1 < n && b[i + 1] == b'\'' {
                        current.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Double-quoted identifier.
        if c == b'"' {
            current.push('"');
            i += 1;
            while i < n {
                current.push(b[i] as char);
                if b[i] == b'"' {
                    if i + 1 < n && b[i + 1] == b'"' {
                        current.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Dollar-quoting: $tag$ ... $tag$
        if c == b'$' {
            // Try to read a dollar-quote opener.
            let start = i;
            let mut tag_end = i + 1;
            while tag_end < n && b[tag_end] != b'$' && b[tag_end].is_ascii_alphanumeric() {
                tag_end += 1;
            }
            if tag_end < n && b[tag_end] == b'$' {
                let tag = &sql[start..=tag_end];
                current.push_str(tag);
                i = tag_end + 1;
                // Find the closing tag.
                while i < n {
                    if b[i] == b'$' && sql[i..].starts_with(tag) {
                        current.push_str(tag);
                        i += tag.len();
                        break;
                    }
                    current.push(b[i] as char);
                    i += 1;
                }
                continue;
            }
        }
        // Semicolon — statement boundary.
        if c == b';' {
            current.push(';');
            let trimmed = current.trim();
            if !trimmed.is_empty() && trimmed != ";" {
                stmts.push(current.clone());
            }
            current.clear();
            i += 1;
            continue;
        }
        current.push(c as char);
        i += 1;
    }
    // Trailing content without a semicolon.
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        stmts.push(current);
    }
    stmts
}

fn strip_explain_query_plan(sql: &str) -> Option<&str> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("explain") {
        return None;
    }
    let after_explain = trimmed.get(7..)?.trim_start();
    let after_explain_lower = after_explain.to_ascii_lowercase();
    if !after_explain_lower.starts_with("query") {
        return None;
    }
    let after_query = after_explain.get(5..)?.trim_start();
    let after_query_lower = after_query.to_ascii_lowercase();
    if !after_query_lower.starts_with("plan") {
        return None;
    }
    Some(after_query.get(4..)?.trim_start())
}

/// Whether `sql` is a `PREPARE`/`EXECUTE`/`DEALLOCATE` statement. These must
/// bypass the session result + logical-plan caches: `EXECUTE name($p)` with
/// varying params would otherwise create one cache entry per parameter set
/// (unbounded growth), and the prepared plan itself is already held by the
/// DataFusion context.
fn is_prepared_stmt_sql(sql: &str) -> bool {
    let lower = sql.trim_start().to_ascii_lowercase();
    lower.starts_with("prepare ")
        || lower.starts_with("execute ")
        || lower.starts_with("deallocate ")
}

/// Parse the first identifier from the text following a `PREPARE`/`DEALLOCATE`
/// keyword (e.g. `"gt AS SELECT ..."` → `"gt"`, `"p"` → `"p"`), stripping
/// surrounding quotes. Used to track prepared-statement names.
fn parse_stmt_ident(s: &str) -> Option<String> {
    let tok = s
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()?;
    let t = tok.trim_matches(|c| c == '"' || c == '`' || c == '\'');
    (!t.is_empty()).then(|| t.to_string())
}

/// Whether `sql` is an `EXPLAIN ANALYZE ...` statement. EXPLAIN ANALYZE falls
/// through to DataFusion 54 (which executes the plan and reports per-operator
/// timing), but its output must never be result-cached: the timing metrics are
/// request-specific and would be misleading on a cache hit.
fn is_explain_analyze(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("explain") {
        return false;
    }
    let after = trimmed[7..].trim_start();
    after.to_ascii_lowercase().starts_with("analyze")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlCompatTokenKind {
    Ident,
    String,
    Dot,
    LParen,
    RParen,
    Comma,
}

#[derive(Debug, Clone)]
struct SqlCompatToken {
    kind: SqlCompatTokenKind,
    raw: String,
    normalized: String,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct FtsMatchBinding {
    query_ref: String,
}

#[derive(Debug, Clone)]
struct SqlReplacement {
    start: usize,
    end: usize,
    replacement: String,
}

fn rewrite_fts_match_compat_sql(sql: &str, db: &Database) -> String {
    let tokens = sql_compat_tokens(sql);
    if tokens.is_empty() {
        return sql.to_string();
    }
    let bindings = fts_match_bindings(sql, db, &tokens);
    if bindings.is_empty() {
        return sql.to_string();
    }
    let unique_refs = bindings
        .values()
        .map(|binding| binding.query_ref.as_str())
        .collect::<HashSet<_>>();
    let unique_binding = if unique_refs.len() == 1 {
        bindings.values().next().cloned()
    } else {
        None
    };
    let mut replacements = Vec::new();
    for (idx, token) in tokens.iter().enumerate() {
        if token.kind != SqlCompatTokenKind::Ident || token.normalized != "match" {
            continue;
        }
        let Some(rhs) = tokens.get(idx + 1) else {
            continue;
        };
        if rhs.kind != SqlCompatTokenKind::String {
            continue;
        }
        let Some((lhs_start, _lhs_end, query_ref)) =
            fts_match_lhs_query_ref(&tokens, idx, &bindings, unique_binding.as_ref())
        else {
            continue;
        };
        replacements.push(SqlReplacement {
            start: lhs_start,
            end: rhs.end,
            replacement: format!("{query_ref}.query = {}", rhs.raw),
        });
    }
    apply_sql_replacements(sql, &replacements)
}

fn fts_match_lhs_query_ref(
    tokens: &[SqlCompatToken],
    match_idx: usize,
    bindings: &HashMap<String, FtsMatchBinding>,
    unique_binding: Option<&FtsMatchBinding>,
) -> Option<(usize, usize, String)> {
    if match_idx == 0 {
        return None;
    }
    let lhs = tokens.get(match_idx - 1)?;
    if lhs.kind != SqlCompatTokenKind::Ident {
        return None;
    }

    if match_idx >= 3
        && tokens.get(match_idx - 2)?.kind == SqlCompatTokenKind::Dot
        && tokens.get(match_idx - 3)?.kind == SqlCompatTokenKind::Ident
    {
        let owner = tokens.get(match_idx - 3)?;
        let binding = bindings.get(&owner.normalized)?;
        if lhs.normalized == "query" || lhs.normalized == "text" {
            return Some((owner.start, lhs.end, binding.query_ref.clone()));
        }
        return None;
    }

    if let Some(binding) = bindings.get(&lhs.normalized) {
        return Some((lhs.start, lhs.end, binding.query_ref.clone()));
    }
    if lhs.normalized == "text" {
        let binding = unique_binding?;
        return Some((lhs.start, lhs.end, binding.query_ref.clone()));
    }
    None
}

fn fts_match_bindings(
    sql: &str,
    db: &Database,
    tokens: &[SqlCompatToken],
) -> HashMap<String, FtsMatchBinding> {
    let mut out = HashMap::new();
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        let starts_table_ref = token.kind == SqlCompatTokenKind::Ident
            && matches!(token.normalized.as_str(), "from" | "join");
        if !starts_table_ref {
            i += 1;
            continue;
        }
        let mut table_idx = i + 1;
        if tokens
            .get(table_idx)
            .is_some_and(|token| token.kind == SqlCompatTokenKind::LParen)
        {
            i += 1;
            continue;
        }
        let Some(table) = tokens.get(table_idx) else {
            break;
        };
        if table.kind != SqlCompatTokenKind::Ident {
            i += 1;
            continue;
        }
        let mut table_name = table.normalized.clone();
        let mut table_ref = table.raw.clone();
        if tokens
            .get(table_idx + 1)
            .is_some_and(|token| token.kind == SqlCompatTokenKind::Dot)
            && tokens
                .get(table_idx + 2)
                .is_some_and(|token| token.kind == SqlCompatTokenKind::Ident)
        {
            let qualified = tokens.get(table_idx + 2).unwrap();
            table_name = qualified.normalized.clone();
            table_ref = sql[table.start..qualified.end].to_string();
            table_idx += 2;
        }
        if !is_fts_docs_table(db, &table_name) {
            i = table_idx + 1;
            continue;
        }
        let mut query_ref = table_ref.clone();
        let mut alias_key = None;
        let mut next = table_idx + 1;
        if tokens.get(next).is_some_and(|token| {
            token.kind == SqlCompatTokenKind::Ident && token.normalized == "as"
        }) {
            next += 1;
        }
        if let Some(alias) = tokens.get(next) {
            if alias.kind == SqlCompatTokenKind::Ident && !is_table_ref_boundary(&alias.normalized)
            {
                alias_key = Some(alias.normalized.clone());
                query_ref = alias.raw.clone();
                next += 1;
            }
        }
        out.insert(
            table_name,
            FtsMatchBinding {
                query_ref: query_ref.clone(),
            },
        );
        if let Some(alias_key) = alias_key {
            out.insert(alias_key, FtsMatchBinding { query_ref });
        }
        i = next;
    }
    out
}

fn is_fts_docs_table(db: &Database, name: &str) -> bool {
    db.external_table(name)
        .is_some_and(|entry| entry.module == "fts_docs")
}

fn is_table_ref_boundary(normalized: &str) -> bool {
    matches!(
        normalized,
        "where"
            | "join"
            | "left"
            | "right"
            | "inner"
            | "outer"
            | "full"
            | "cross"
            | "on"
            | "using"
            | "group"
            | "order"
            | "having"
            | "limit"
            | "offset"
            | "union"
            | "except"
            | "intersect"
    )
}

fn sql_compat_tokens(sql: &str) -> Vec<SqlCompatToken> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b if b.is_ascii_whitespace() => i += 1,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i = skip_block_comment(bytes, i);
            }
            b'\'' => {
                let end = skip_quoted(bytes, i, b'\'');
                tokens.push(sql_token(sql, SqlCompatTokenKind::String, i, end));
                i = end;
            }
            b'E' | b'e' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => {
                let end = skip_quoted(bytes, i + 1, b'\'');
                tokens.push(sql_token(sql, SqlCompatTokenKind::String, i, end));
                i = end;
            }
            b'$' => {
                let (end, matched) = skip_dollar_quoted(bytes, i);
                if matched {
                    tokens.push(sql_token(sql, SqlCompatTokenKind::String, i, end));
                    i = end;
                } else {
                    i += 1;
                }
            }
            b'"' => {
                let end = skip_quoted(bytes, i, b'"');
                let raw = sql[i..end].to_string();
                let normalized = unquote_sql_ident(&raw).to_ascii_lowercase();
                tokens.push(SqlCompatToken {
                    kind: SqlCompatTokenKind::Ident,
                    raw,
                    normalized,
                    start: i,
                    end,
                });
                i = end;
            }
            b'.' => {
                tokens.push(sql_token(sql, SqlCompatTokenKind::Dot, i, i + 1));
                i += 1;
            }
            b'(' => {
                tokens.push(sql_token(sql, SqlCompatTokenKind::LParen, i, i + 1));
                i += 1;
            }
            b')' => {
                tokens.push(sql_token(sql, SqlCompatTokenKind::RParen, i, i + 1));
                i += 1;
            }
            b',' => {
                tokens.push(sql_token(sql, SqlCompatTokenKind::Comma, i, i + 1));
                i += 1;
            }
            b if is_sql_ident_byte(b) => {
                let start = i;
                i += 1;
                while i < bytes.len() && is_sql_ident_byte(bytes[i]) {
                    i += 1;
                }
                tokens.push(sql_token(sql, SqlCompatTokenKind::Ident, start, i));
            }
            _ => i += 1,
        }
    }
    tokens
}

fn sql_token(sql: &str, kind: SqlCompatTokenKind, start: usize, end: usize) -> SqlCompatToken {
    let raw = sql[start..end].to_string();
    SqlCompatToken {
        kind,
        normalized: raw.to_ascii_lowercase(),
        raw,
        start,
        end,
    }
}

fn unquote_sql_ident(raw: &str) -> String {
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_string()
    }
}

fn apply_sql_replacements(sql: &str, replacements: &[SqlReplacement]) -> String {
    if replacements.is_empty() {
        return sql.to_string();
    }
    let mut ordered = replacements.to_vec();
    ordered.sort_by_key(|replacement| replacement.start);
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0;
    for replacement in ordered {
        if replacement.start < cursor || replacement.end > sql.len() {
            continue;
        }
        out.push_str(&sql[cursor..replacement.start]);
        out.push_str(&replacement.replacement);
        cursor = replacement.end;
    }
    out.push_str(&sql[cursor..]);
    out
}

fn rewrite_compat_function_calls(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = copy_quoted_to_string(&mut out, bytes, i, b'\''),
            b'"' => i = copy_quoted_to_string(&mut out, bytes, i, b'"'),
            b'E' | b'e' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => {
                out.push(bytes[i] as char);
                i += 1;
                i = copy_quoted_to_string(&mut out, bytes, i, b'\'');
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                out.push('-');
                out.push('-');
                i += 2;
                while i < bytes.len() {
                    let ch = bytes[i] as char;
                    out.push(ch);
                    i += 1;
                    if ch == '\n' {
                        break;
                    }
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                let start = i;
                i = skip_block_comment(bytes, i);
                out.push_str(&sql[start..i.min(bytes.len())]);
            }
            b'$' => {
                let start_len = out.len();
                let (next, matched) = copy_dollar_quoted_to_string(&mut out, bytes, i);
                if matched {
                    i = next;
                } else {
                    out.truncate(start_len);
                    out.push('$');
                    i += 1;
                }
            }
            b'g' | b'G' | b'm' | b'M' | b't' | b'T' => {
                if let Some((replacement, next)) = compat_function_rewrite_at(sql, i) {
                    out.push_str(&replacement);
                    i = next;
                } else {
                    out.push(bytes[i] as char);
                    i += 1;
                }
            }
            _ => {
                out.push(bytes[i] as char);
                i += 1;
            }
        }
    }
    out
}

fn compat_function_rewrite_at(sql: &str, start: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let (name, kind) = if ident_eq_at(bytes, start, b"max") {
        ("max", CompatRewriteKind::ScalarMax)
    } else if ident_eq_at(bytes, start, b"min") {
        ("min", CompatRewriteKind::ScalarMin)
    } else if ident_eq_at(bytes, start, b"group_concat") {
        ("group_concat", CompatRewriteKind::GroupConcat)
    } else if ident_eq_at(bytes, start, b"total") {
        ("total", CompatRewriteKind::Total)
    } else {
        return None;
    };
    let before_ok = start == 0 || !is_sql_ident_byte(bytes[start - 1]);
    let after_name = start + name.len();
    let after_ok = bytes
        .get(after_name)
        .is_some_and(|b| !is_sql_ident_byte(*b));
    if !before_ok || !after_ok {
        return None;
    }
    let mut open = after_name;
    while open < bytes.len() && bytes[open].is_ascii_whitespace() {
        open += 1;
    }
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let summary = call_arg_summary(sql, open)?;
    match kind {
        CompatRewriteKind::ScalarMax if summary.top_level_commas > 0 => {
            Some(("__mongreldb_scalar_max(".to_string(), open + 1))
        }
        CompatRewriteKind::ScalarMin if summary.top_level_commas > 0 => {
            Some(("__mongreldb_scalar_min(".to_string(), open + 1))
        }
        CompatRewriteKind::GroupConcat => {
            let args = &sql[open + 1..summary.close];
            let rewritten = if summary.top_level_commas == 0 {
                format!("string_agg({args}, ',')")
            } else {
                format!("string_agg({args})")
            };
            Some((rewritten, summary.close + 1))
        }
        CompatRewriteKind::Total if summary.top_level_commas == 0 => {
            let args = &sql[open + 1..summary.close];
            let suffix_end = aggregate_suffix_end(sql, summary.close + 1);
            let suffix = &sql[summary.close + 1..suffix_end];
            Some((
                format!("coalesce(cast(sum({args}){suffix} as double), 0.0)"),
                suffix_end,
            ))
        }
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum CompatRewriteKind {
    ScalarMax,
    ScalarMin,
    GroupConcat,
    Total,
}

fn ident_eq_at(bytes: &[u8], start: usize, ident: &[u8]) -> bool {
    bytes
        .get(start..start + ident.len())
        .is_some_and(|slice| slice.eq_ignore_ascii_case(ident))
}

fn is_sql_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

fn keyword_at(bytes: &[u8], start: usize, keyword: &[u8]) -> bool {
    if !ident_eq_at(bytes, start, keyword) {
        return false;
    }
    let before_ok = start == 0 || !is_sql_ident_byte(bytes[start - 1]);
    let after = start + keyword.len();
    let after_ok = after >= bytes.len() || !is_sql_ident_byte(bytes[after]);
    before_ok && after_ok
}

fn skip_sql_whitespace(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn aggregate_suffix_end(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut suffix_end = start;
    let mut i = skip_sql_whitespace(bytes, start);

    if keyword_at(bytes, i, b"filter") {
        let open = skip_sql_whitespace(bytes, i + b"filter".len());
        if bytes.get(open) != Some(&b'(') {
            return start;
        }
        let Some(summary) = call_arg_summary(sql, open) else {
            return start;
        };
        suffix_end = summary.close + 1;
        i = skip_sql_whitespace(bytes, suffix_end);
    }

    if keyword_at(bytes, i, b"over") {
        let after_over = skip_sql_whitespace(bytes, i + b"over".len());
        if bytes.get(after_over) == Some(&b'(') {
            let Some(summary) = call_arg_summary(sql, after_over) else {
                return suffix_end;
            };
            suffix_end = summary.close + 1;
        } else {
            let mut end = after_over;
            while end < bytes.len() && is_sql_ident_byte(bytes[end]) {
                end += 1;
            }
            if end > after_over {
                suffix_end = end;
            }
        }
    }

    suffix_end
}

struct CallArgSummary {
    close: usize,
    top_level_commas: usize,
}

fn call_arg_summary(sql: &str, open: usize) -> Option<CallArgSummary> {
    let bytes = sql.as_bytes();
    let mut depth = 1;
    let mut i = open + 1;
    let mut top_level_commas = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_quoted(bytes, i, b'\''),
            b'"' => i = skip_quoted(bytes, i, b'"'),
            b'E' | b'e' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => {
                i = skip_quoted(bytes, i + 1, b'\'')
            }
            b'$' => {
                let (next, matched) = skip_dollar_quoted(bytes, i);
                i = if matched { next } else { i + 1 };
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i = skip_block_comment(bytes, i);
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(CallArgSummary {
                        close: i,
                        top_level_commas,
                    });
                }
                i += 1;
            }
            b',' if depth == 1 => {
                top_level_commas += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

fn copy_quoted_to_string(out: &mut String, bytes: &[u8], start: usize, delim: u8) -> usize {
    let end = skip_quoted(bytes, start, delim);
    out.push_str(std::str::from_utf8(&bytes[start..end]).unwrap_or_default());
    end
}

fn skip_quoted(bytes: &[u8], start: usize, delim: u8) -> usize {
    let mut i = start;
    if i < bytes.len() {
        i += 1;
    }
    while i < bytes.len() {
        if bytes[i] == delim {
            i += 1;
            if i < bytes.len() && bytes[i] == delim {
                i += 1;
                continue;
            }
            break;
        }
        i += 1;
    }
    i
}

fn copy_dollar_quoted_to_string(out: &mut String, bytes: &[u8], start: usize) -> (usize, bool) {
    let (end, matched) = skip_dollar_quoted(bytes, start);
    if matched {
        out.push_str(std::str::from_utf8(&bytes[start..end]).unwrap_or_default());
    }
    (end, matched)
}

fn skip_dollar_quoted(bytes: &[u8], start: usize) -> (usize, bool) {
    if bytes.get(start) != Some(&b'$') {
        return (start, false);
    }
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if bytes.get(j) != Some(&b'$') {
        return (start, false);
    }
    let tag = &bytes[start..=j];
    let mut i = j + 1;
    while i + tag.len() <= bytes.len() {
        if &bytes[i..i + tag.len()] == tag {
            return (i + tag.len(), true);
        }
        i += 1;
    }
    (start, false)
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    let mut depth = 1;
    while i + 1 < bytes.len() && depth > 0 {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
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
            default_value: None,
        });
    }

    Ok((
        name,
        Schema {
            schema_id,
            columns,
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
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
    fn bounded_lru_evicts_least_recently_used() {
        let mut cache = BoundedLru::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        assert_eq!(cache.get("a"), Some(&1));
        cache.insert("c", 3);
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.get("b"), None);
        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.get("c"), Some(&3));
    }

    #[tokio::test]
    async fn streaming_query_bypasses_result_cache() {
        use futures::StreamExt;

        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(database).unwrap();
        let mut stream = session.run_stream("SELECT 1 AS value").await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(stream.next().await.is_none());
        assert!(session.cache.lock().entries.is_empty());
    }

    #[tokio::test]
    async fn ttl_tables_bypass_epoch_keyed_result_cache() {
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(database).unwrap();
        session
            .run(
                "CREATE TABLE events (id BIGINT PRIMARY KEY, ts TIMESTAMP) \
                 TTL_COLUMN ts TTL '1 day'",
            )
            .await
            .unwrap();
        session.run("SELECT * FROM events").await.unwrap();
        assert!(session.cache.lock().entries.is_empty());
    }

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

    #[test]
    fn compat_function_rewrite_handles_sqlite_compatibility_calls() {
        assert_eq!(
            rewrite_compat_function_calls("select max(id), min(id) from t"),
            "select max(id), min(id) from t"
        );
        assert_eq!(
            rewrite_compat_function_calls("select max(1, min(2, 3), 'max(4,5)')"),
            "select __mongreldb_scalar_max(1, __mongreldb_scalar_min(2, 3), 'max(4,5)')"
        );
        assert_eq!(
            rewrite_compat_function_calls("select /* max(1,2) */ min(1, (2 + 3))"),
            "select /* max(1,2) */ __mongreldb_scalar_min(1, (2 + 3))"
        );
        assert_eq!(
            rewrite_compat_function_calls("select max_value, min_value from t"),
            "select max_value, min_value from t"
        );
        assert_eq!(
            rewrite_compat_function_calls(
                "select group_concat(label), group_concat(label, '|') from t"
            ),
            "select string_agg(label, ','), string_agg(label, '|') from t"
        );
        assert_eq!(
            rewrite_compat_function_calls("select total(val), total(val) filter (where grp = 2) from t"),
            "select coalesce(cast(sum(val) as double), 0.0), coalesce(cast(sum(val) filter (where grp = 2) as double), 0.0) from t"
        );
        assert_eq!(
            rewrite_compat_function_calls(
                "select total(val) over (partition by grp order by id) from t"
            ),
            "select coalesce(cast(sum(val) over (partition by grp order by id) as double), 0.0) from t"
        );
    }
}
