//! Native aggregate fast path (Phase 7.2).
//!
//! For the common analytical shape `SELECT <agg>(col) FROM <primary> [WHERE ...]`
//! (single aggregate, no `GROUP BY`, agg = COUNT/`COUNT(*)`/SUM/MIN/MAX/AVG over
//! a bare Int64/Float64 column), compute the result directly over the page-pruned
//! native cursor in [`mongreldb_core::Table::aggregate_native`] — one vectorized
//! pass, no `Value`, no Arrow `RecordBatch` materialized for the input. Anything
//! more involved (joins, `GROUP BY`, expressions, secondary tables, unsupported
//! predicates) falls through to normal DataFusion execution.
//!
//! The intercept works off the (unoptimized) logical plan in `MongrelSession::run`,
//! so it never fights DataFusion's optimizer; correctness is exact because the
//! fast path only fires when every WHERE conjunct translates to a MongrelDB index
//! [`Condition`] (otherwise DataFusion handles it).

use arrow::array::{ArrayRef, Float64Builder, Int64Builder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use mongreldb_core::{Condition, NativeAgg, Schema, Snapshot, Table, TypeId};
use std::sync::Arc;

use crate::error::{MongrelQueryError, Result};

/// If `plan` is a servable single-aggregate query over the primary table, run it
/// natively and return the one-row result batch; otherwise `None` (fall through
/// to DataFusion).
pub(crate) fn try_native_aggregate(
    db: &mut Table,
    schema: &Schema,
    _snapshot: Snapshot,
    plan: &LogicalPlan,
    cache_key: u64,
) -> Result<Option<RecordBatch>> {
    let Some(agg) = peel_to_aggregate(plan) else {
        return Ok(None);
    };
    if !agg.group_expr.is_empty() || agg.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let core_err = |e: mongreldb_core::MongrelError| MongrelQueryError::Core(e);

    // Phase 7.1c: COUNT(DISTINCT col) over a bitmap-indexed column with no WHERE
    // is the bitmap's distinct-key count — no scan. Falls through to DataFusion
    // when there is a filter, no bitmap index, or the table isn't insert-only.
    if let Some(col_name) = parse_count_distinct(&agg.aggr_expr[0]) {
        let unfiltered = extract_filter_conjuncts(&agg.input).is_some_and(|c| c.is_empty());
        if unfiltered {
            if let Some(cdef) = schema.columns.iter().find(|c| c.name == col_name) {
                if let Some(n) = db.count_distinct_from_bitmap(cdef.id).map_err(core_err)? {
                    let out_schema: SchemaRef = Arc::new(arrow_schema_from_df(&agg.schema));
                    let array = scalar_to_array(&ScalarValue::Int64(Some(n as i64)));
                    let batch = RecordBatch::try_new(out_schema, vec![array])
                        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
                    return Ok(Some(batch));
                }
            }
        }
        return Ok(None); // a DISTINCT shape we can't serve ⇒ let DataFusion run it
    }

    let Some((agg_kind, col_name)) = parse_agg_expr(&agg.aggr_expr[0]) else {
        return Ok(None);
    };

    // Resolve the aggregate column (None for COUNT(*)). Only Int64/Float64.
    let column: Option<u16> = match &col_name {
        Some(name) => match schema.columns.iter().find(|c| c.name == *name) {
            Some(c)
                if matches!(
                    c.ty,
                    TypeId::Int64 | TypeId::Float64 | TypeId::TimestampNanos | TypeId::Date32
                ) =>
            {
                Some(c.id)
            }
            _ => return Ok(None),
        },
        None => None,
    };

    let Some(conjuncts) = extract_filter_conjuncts(&agg.input) else {
        return Ok(None); // not a single TableScan shape
    };
    let Some(translated) = translate_all(&conjuncts, schema) else {
        return Ok(None); // a conjunct didn't translate ⇒ let DataFusion handle it
    };
    // The native path REPLACES the query (no post-filter re-application), so it
    // may only fire for Exact pushdowns. FmContains (LIKE) is an inexact
    // substring superset — defer to DataFusion, which re-applies the wildcard.
    if translated
        .iter()
        .any(|c| matches!(c, Condition::FmContains { .. }))
    {
        return Ok(None);
    }

    // Phase 8.3: serve from the incremental aggregate cache — warm cache ⇒
    // delta merge; cold ⇒ vectorized cursor / visible-rows scan that seeds the
    // cache. `aggregate_incremental` always returns a state (it falls back to a
    // full scan for multi-run/memtable layouts, extending native coverage).
    let result = db
        .aggregate_incremental(cache_key, &translated, column, agg_kind)
        .map_err(core_err)?;

    // Build a one-row batch matching the aggregate's output field type.
    let out_schema: SchemaRef = Arc::new(arrow_schema_from_df(&agg.schema));
    let out_is_int = matches!(
        agg.schema.fields().first().map(|f| f.data_type()),
        Some(arrow::datatypes::DataType::Int64)
    );
    let val = scalar_for_state(result.state, out_is_int);
    let array = scalar_to_array(&val);
    let batch = RecordBatch::try_new(out_schema, vec![array])
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
    Ok(Some(batch))
}

/// Peel an outer `Projection` (e.g. the `as n` alias wrapper) to reach an
/// `Aggregate` node.
fn peel_to_aggregate(plan: &LogicalPlan) -> Option<&datafusion::logical_expr::Aggregate> {
    match plan {
        LogicalPlan::Aggregate(a) => Some(a),
        LogicalPlan::Projection(p) => peel_to_aggregate(&p.input),
        _ => None,
    }
}

/// Parse `agg_expr` (peeling an `Alias`) into `(NativeAgg, Option<column name>)`.
/// `None` for unsupported shapes.
fn parse_agg_expr(expr: &Expr) -> Option<(NativeAgg, Option<String>)> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        if let Expr::Alias(alias) = expr {
            return parse_agg_expr(&alias.expr);
        }
        return None;
    };
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() {
        return None;
    }
    let agg = match func.name() {
        "count" => NativeAgg::Count,
        "sum" => NativeAgg::Sum,
        "min" => NativeAgg::Min,
        "max" => NativeAgg::Max,
        "avg" => NativeAgg::Avg,
        _ => return None,
    };
    let col = match (agg, params.args.as_slice()) {
        (NativeAgg::Count, []) => None, // COUNT(*)
        (_, [Expr::Column(c)]) => Some(c.name.clone()),
        _ => return None,
    };
    Some((agg, col))
}

/// If `expr` is `COUNT(DISTINCT <column>)` (peeling an `Alias`), return the
/// column name. `None` for any other shape (incl. `COUNT(DISTINCT expr)` or a
/// `FILTER`/`ORDER BY` clause).
fn parse_count_distinct(expr: &Expr) -> Option<String> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        if let Expr::Alias(alias) = expr {
            return parse_count_distinct(&alias.expr);
        }
        return None;
    };
    if func.name() != "count"
        || !params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
    {
        return None;
    }
    match params.args.as_slice() {
        [Expr::Column(c)] => Some(c.name.clone()),
        _ => None,
    }
}

/// From `Aggregate.input = (Filter)? → TableScan`, return the WHERE conjuncts
/// (empty if no WHERE). `None` if the shape is not a single TableScan.
fn extract_filter_conjuncts(input: &LogicalPlan) -> Option<Vec<Expr>> {
    match input {
        LogicalPlan::Filter(f) => {
            if !matches!(f.input.as_ref(), LogicalPlan::TableScan(_)) {
                return None;
            }
            Some(split_and(&f.predicate))
        }
        LogicalPlan::TableScan(_) => Some(Vec::new()),
        _ => None,
    }
}

/// Split an `Expr` on AND into its conjuncts.
fn split_and(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            let mut v = split_and(&b.left);
            v.extend(split_and(&b.right));
            v
        }
        other => vec![other.clone()],
    }
}

/// Translate every conjunct; `None` if any does not fully translate.
fn translate_all(exprs: &[Expr], schema: &Schema) -> Option<Vec<Condition>> {
    exprs
        .iter()
        .map(|e| crate::translate_filter(e, schema))
        .collect()
}

/// Map the mergeable aggregate state to the matching Arrow [`ScalarValue`].
/// `out_is_int` picks Int64 vs Float64 to match the aggregate's output schema
/// (COUNT and Int64 SUM/MIN/MAX ⇒ Int64; Float64 columns and AVG ⇒ Float64);
/// an empty state (no inputs) ⇒ null.
fn scalar_for_state(state: mongreldb_core::AggState, out_is_int: bool) -> ScalarValue {
    match state.point() {
        Some(v) if out_is_int => ScalarValue::Int64(Some(v as i64)),
        Some(v) => ScalarValue::Float64(Some(v)),
        None if out_is_int => ScalarValue::Int64(None),
        None => ScalarValue::Float64(None),
    }
}

/// Build a single-element Arrow array from a scalar.
fn scalar_to_array(val: &ScalarValue) -> ArrayRef {
    match val {
        ScalarValue::Float64(opt) => {
            let mut b = Float64Builder::new();
            match opt {
                Some(x) => b.append_value(*x),
                None => b.append_null(),
            }
            Arc::new(b.finish())
        }
        ScalarValue::Int64(opt) => {
            let mut b = Int64Builder::new();
            match opt {
                Some(x) => b.append_value(*x),
                None => b.append_null(),
            }
            Arc::new(b.finish())
        }
        _ => {
            let mut b = Int64Builder::new();
            b.append_null();
            Arc::new(b.finish())
        }
    }
}

/// Convert the DataFusion `DFSchema` of the aggregate output into an Arrow schema.
fn arrow_schema_from_df(df_schema: &datafusion::common::DFSchema) -> arrow::datatypes::Schema {
    let fields: Vec<arrow::datatypes::Field> = df_schema
        .fields()
        .iter()
        .map(|f| arrow::datatypes::Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
        .collect();
    arrow::datatypes::Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MongrelProvider;
    use arrow::array::Int64Array;
    use datafusion::prelude::SessionContext;
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema as MSchema, TypeId};
    use mongreldb_core::{Table, Value};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn schema() -> MSchema {
        MSchema {
            schema_id: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                },
                ColumnDef {
                    id: 2,
                    name: "v".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty(),
                },
            ],
            indexes: Vec::new(),
            colocation: vec![],
        }
    }

    async fn ctx_and_db() -> (SessionContext, Arc<Mutex<Table>>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        for i in 0..1000i64 {
            db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 2))])
                .unwrap();
        }
        db.flush().unwrap();
        let db = Arc::new(Mutex::new(db));
        let provider = MongrelProvider::new(Arc::clone(&db)).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        (ctx, db, dir)
    }

    #[tokio::test]
    async fn matcher_fires_for_aggregate_not_for_select() {
        let (ctx, db, _dir) = ctx_and_db().await;
        let sum_plan = ctx.sql("select sum(v) as s from t").await.unwrap();
        let sel_plan = ctx.sql("select v from t").await.unwrap();
        let mut g = db.lock();
        let schema = g.schema().clone();
        let snap = g.snapshot();
        let fired_sum =
            try_native_aggregate(&mut g, &schema, snap, sum_plan.logical_plan(), 1).unwrap();
        assert!(fired_sum.is_some(), "SUM should fire the native path");
        let fired_sel =
            try_native_aggregate(&mut g, &schema, snap, sel_plan.logical_plan(), 2).unwrap();
        assert!(fired_sel.is_none(), "a plain SELECT must fall through");
        // The fired value is correct: sum(v)=sum(0,2,..,1998)=2*sum(0..1000)=999000.
        let b = fired_sum.unwrap();
        let v = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 2 * (0..1000i64).sum::<i64>());
    }
}
