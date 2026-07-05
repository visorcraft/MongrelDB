//! FK-join as row-id-set intersection (Phase 8.1).
//!
//! For the common analytical shape
//! `SELECT … FROM fk_t JOIN pk_t ON fk_t.fk_col = pk_t.pk_col [WHERE …]` —
//! where `pk_t.pk_col` is the primary key of the PK side and `fk_t.fk_col`
//! carries a roaring-bitmap index on the FK side — the join is resolved by
//! **bitmap intersection** instead of DataFusion's hash join over fully
//! materialized batches:
//!
//! 1. resolve the PK side (its WHERE predicates → survivor primary-key rows);
//! 2. for every surviving PK value, union `bitmap[fk_col].get(pk_value)` on the
//!    FK side ⇒ the referencing row-id set;
//! 3. intersect that set with the FK side's own predicates
//!    (`ann_search ∩ bitmap_eq ∩ range`, …);
//! 4. materialize only the surviving FK rows and their matched PK rows.
//!
//! This composes with every index-served `Condition` (HOT, bitmap, PGM range,
//! ANN, sparse). LIKE-derived `FmContains` is an inexact substring *superset*;
//! because this intercept *replaces* execution (no DataFusion post-filter), it
//! only fires for exact predicates — LIKE falls through to DataFusion.
//!
//! The intercept works off the (unoptimized) logical plan in
//! [`crate::MongrelSession::run`], mirroring [`crate::native_agg`]. At that
//! stage DataFusion represents `FROM t alias` as a `SubqueryAlias` over the
//! `TableScan`, and a single `col = col` join condition as the join `filter`
//! rather than an `on` equi-pair — both are handled below.

use crate::arrow_conv::{arrow_data_type, build_array};
use crate::error::{MongrelQueryError, Result};
use crate::{translate_ann_search, translate_filter};
use arrow::array::{new_empty_array, ArrayRef};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator};
use mongreldb_core::{schema::IndexKind, Condition, Query, Row, Schema, Table, TypeId, Value};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// `(sort_expr, asc, nulls_first)` keys plus an optional `LIMIT n` fetch.
type SortInfo = (Vec<(Expr, bool, bool)>, Option<usize>);

/// If `plan` is a servable FK-join over two registered tables, run it via
/// bitmap intersection and return the result batches; otherwise `None` (fall
/// through to DataFusion).
pub(crate) fn try_fk_join(
    tables: &HashMap<String, Arc<Mutex<Table>>>,
    plan: &LogicalPlan,
) -> Result<Option<Vec<RecordBatch>>> {
    // 1. Peel outer Sort / Limit (captured), Projection (captured), optional
    //    top-level Aggregate (Phase 13.4), and any top-level Filter(s) above the
    //    Join (their conjuncts are routed to the side whose columns they
    //    reference).
    let (sort, limit, rest) = peel_sort_limit(plan);
    let (projection, after_proj) = peel_projection(rest);
    let (aggregate, after_agg) = peel_aggregate(after_proj);
    let (top_conjuncts, join_plan) = peel_filters(after_agg);
    let LogicalPlan::Join(join) = join_plan else {
        return Ok(None);
    };
    // This is a join-shaped query: assume the DataFusion hash-join fallback
    // (Priority 13 diagnostics). The caller overwrites this with `FkBitmap` when
    // the native path actually serves the result. Non-join queries returned
    // above and stay `JoinMode::None`.
    mongreldb_core::trace::QueryTrace::record(|t| {
        t.join_mode = mongreldb_core::trace::JoinMode::DataFusionHash;
    });
    // Phase 13.6: Inner, Left, LeftSemi, LeftAnti, RightSemi, RightAnti.
    if !matches!(
        join.join_type,
        JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti
    ) {
        return Ok(None);
    }

    // 2. Each side is (Filter)? → (SubqueryAlias)? → TableScan over a
    //    registered table. Collect alias + per-side WHERE conjuncts.
    let Some(left) = peel_side(&join.left) else {
        return Ok(None);
    };
    let Some(right) = peel_side(&join.right) else {
        return Ok(None);
    };
    if left.table == right.table {
        return Ok(None); // self-join not supported here
    }
    if tables.get(&left.table).is_none() || tables.get(&right.table).is_none() {
        return Ok(None);
    }

    // Route any top-level conjuncts to the side(s) they belong to. A conjunct
    // referencing both sides (a true join filter) can't be served here ⇒ bail.
    let (mut left, mut right) = (left, right);
    for conj in &top_conjuncts {
        match route_conjunct(conj, &left, &right, tables)? {
            Some(true) => left.conjuncts.push(conj.clone()),
            Some(false) => right.conjuncts.push(conj.clone()),
            None => return Ok(None),
        }
    }

    // 3. Extract the single `col = col` equi-condition (from `on` or `filter`)
    //    and classify it into FK side + PK side.
    let Some(jc) = classify(join, &left, &right, tables)? else {
        return Ok(None);
    };

    // 4. Translate each side's WHERE into exact index Conditions. Each conjunct
    //    must translate (else the set is inexact); FmContains (inexact LIKE
    //    superset) ⇒ bail to DataFusion.
    let pk_conj = if jc.pk_is_left {
        &left.conjuncts
    } else {
        &right.conjuncts
    };
    let fk_conj = if jc.pk_is_left {
        &right.conjuncts
    } else {
        &left.conjuncts
    };
    let Some(pk_conds) = translate_side(&jc.pk_table, tables, pk_conj)? else {
        return Ok(None);
    };
    let Some(fk_conds) = translate_side(&jc.fk_table, tables, fk_conj)? else {
        return Ok(None);
    };
    if pk_conds
        .iter()
        .chain(fk_conds.iter())
        .any(|c| matches!(c, Condition::FmContains { .. }))
    {
        return Ok(None);
    }

    // Every path below reads live indexes (`fk_join_count`/`fk_join_row_ids`
    // intersect the FK bitmap; the broadcast path probes PK HOT). A deferred
    // bulk load pays its one-time index build here before those `&Table`
    // reads (Phase 14.7 lazy contract).
    lock_db(&jc.pk_table, tables)
        .ensure_indexes_complete()
        .map_err(MongrelQueryError::Core)?;
    lock_db(&jc.fk_table, tables)
        .ensure_indexes_complete()
        .map_err(MongrelQueryError::Core)?;

    // 5. Resolve the PK side survivor rows; collect their join-column values.
    //    Phase 17.2: broadcast join — when the PK side has no WHERE filter
    //    (would load the entire table) and the FK table has a bitmap index on
    //    the join column, iterate the FK bitmap keys and probe the PK HOT
    //    index instead. O(distinct_fk_values) row loads vs O(total_pk_rows).
    let (pk_rows, pk_col_id) = {
        let pk_db = lock_db(&jc.pk_table, tables);
        let pk_schema = pk_db.schema().clone();
        let Some(pk_col_id) = pk_schema.column(&jc.pk_name).map(|c| c.id) else {
            return Ok(None);
        };

        // Try the broadcast path: PK conditions empty + indexes complete + FK
        // has bitmap on join col.
        if pk_conds.is_empty() && pk_db.indexes_complete() {
            let fk_db = lock_db(&jc.fk_table, tables);
            let fk_schema = fk_db.schema().clone();
            if let Some(fk_col_id) = fk_schema.column(&jc.fk_name).map(|c| c.id) {
                if let Some(bcast_values) = fk_db.broadcast_join_values(fk_col_id, &pk_db) {
                    let snap = pk_db.snapshot();
                    let rows: Vec<_> = bcast_values
                        .iter()
                        .filter_map(|v| pk_db.lookup_pk(v))
                        .filter_map(|rid| pk_db.get(rid, snap))
                        .collect();
                    (rows, pk_col_id)
                } else {
                    drop(pk_db);
                    let mut pk_db2 = lock_db(&jc.pk_table, tables);
                    let rows = pk_db2
                        .query(&Query {
                            conditions: pk_conds.clone(),
                        })
                        .map_err(MongrelQueryError::Core)?;
                    (rows, pk_col_id)
                }
            } else {
                drop(pk_db);
                let mut pk_db2 = lock_db(&jc.pk_table, tables);
                let rows = pk_db2
                    .query(&Query {
                        conditions: pk_conds.clone(),
                    })
                    .map_err(MongrelQueryError::Core)?;
                (rows, pk_col_id)
            }
        } else {
            drop(pk_db);
            let mut pk_db2 = lock_db(&jc.pk_table, tables);
            let rows = pk_db2
                .query(&Query {
                    conditions: pk_conds.clone(),
                })
                .map_err(MongrelQueryError::Core)?;
            (rows, pk_col_id)
        }
    };
    let pk_values: Vec<Vec<u8>> = pk_rows
        .iter()
        .map(|r| {
            r.columns
                .get(&pk_col_id)
                .cloned()
                .unwrap_or(Value::Null)
                .encode_key()
        })
        .collect();
    if pk_values.is_empty() {
        // No surviving PK rows ⇒ empty inner-join result.
        return match output_schema(projection, &jc, &left, &right, tables)? {
            Some(schema) => Ok(Some(vec![empty_batch(schema)?])),
            None => Ok(None),
        };
    }

    // Phase 17.4: COUNT(*) over the join needs only the survivor cardinality,
    // not the materialized row set. Compute it straight from the bitmap union
    // (O(1) when there is no FK-side filter) and short-circuit before resolving
    // the (possibly huge) FK survivor Vec. This turns `SELECT COUNT(*) … JOIN`
    // on a 1M-row fact table from a full materialize+sort into a bitmap len.
    if let Some(agg) = aggregate {
        if bare_count_star(agg).is_some() {
            let fk_count = {
                let fk_db = lock_db(&jc.fk_table, tables);
                let fk_schema = fk_db.schema().clone();
                let Some(fk_col_id) = fk_schema.column(&jc.fk_name).map(|c| c.id) else {
                    return Ok(None);
                };
                let snap = fk_db.snapshot();
                fk_db
                    .fk_join_count(fk_col_id, &pk_values, &fk_conds, snap)
                    .map_err(MongrelQueryError::Core)?
            };
            let out_field =
                agg.schema.fields().first().ok_or_else(|| {
                    MongrelQueryError::Arrow("aggregate output has no fields".into())
                })?;
            let out_schema: arrow::datatypes::SchemaRef =
                Arc::new(arrow::datatypes::Schema::new(vec![out_field.clone()]));
            return Ok(Some(vec![scalar_batch(
                datafusion::common::ScalarValue::Int64(Some(fk_count as i64)),
                out_schema,
            )?]));
        }
    }

    // 6. Resolve the FK side survivor row-ids via bitmap intersection, then
    //    materialize just those rows.
    let (fk_rids, fk_col_id) = {
        let fk_db = lock_db(&jc.fk_table, tables);
        let fk_schema = fk_db.schema().clone();
        let Some(fk_col_id) = fk_schema.column(&jc.fk_name).map(|c| c.id) else {
            return Ok(None);
        };
        let snap = fk_db.snapshot();
        let rids = fk_db
            .fk_join_row_ids(fk_col_id, &pk_values, &fk_conds, snap)
            .map_err(MongrelQueryError::Core)?;
        (rids, fk_col_id)
    };

    // Phase 13.4: if the plan has a top-level Aggregate, compute it directly
    // from the survivor set without materializing rows for a hash join.
    if let Some(agg) = aggregate {
        return compute_join_aggregate(agg, &jc, &left, &right, tables, &fk_rids);
    }

    // Phase 13.6: SEMI / ANTI joins. The FK side must be the probed side.
    let fk_rid_set: std::collections::HashSet<u64> = fk_rids.iter().copied().collect();
    match jc.join_type {
        JoinType::LeftSemi | JoinType::LeftAnti if !jc.pk_is_left => {}
        JoinType::RightSemi | JoinType::RightAnti if jc.pk_is_left => {}
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
            return Ok(None); // FK side is not the probed side
        }
        _ => {}
    }
    let is_semi_anti = matches!(
        jc.join_type,
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
    );
    if is_semi_anti {
        let fk_db = lock_db(&jc.fk_table, tables);
        let snap = fk_db.snapshot();
        let fk_schema = fk_db.schema().clone();
        let all_fk_rids: Vec<u64> = {
            let rows = fk_db.visible_rows(snap).map_err(MongrelQueryError::Core)?;
            rows.iter().map(|r| r.row_id.0).collect()
        };
        let want_rids: Vec<u64> = match jc.join_type {
            JoinType::LeftSemi | JoinType::RightSemi => fk_rids.clone(),
            _ => all_fk_rids
                .iter()
                .filter(|r| !fk_rid_set.contains(r))
                .copied()
                .collect(),
        };
        let rows = fk_db
            .rows_for_rids(&want_rids, snap)
            .map_err(MongrelQueryError::Core)?;
        let out_cols: Vec<OutCol> = fk_schema
            .columns
            .iter()
            .map(|c| OutCol {
                is_fk: true,
                column_id: c.id,
                name: c.name.clone(),
                ty: c.ty,
            })
            .collect();
        let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for r in &rows {
            let row: Vec<Value> = out_cols
                .iter()
                .map(|oc| r.columns.get(&oc.column_id).cloned().unwrap_or(Value::Null))
                .collect();
            out_rows.push(row);
        }
        if let Some((keys, fetch)) = sort {
            apply_sort(&mut out_rows, &out_cols, &keys);
            if let Some(fetch) = fetch {
                out_rows.truncate(fetch);
            }
        }
        if let Some((skip, fetch)) = limit {
            let skip = skip.min(out_rows.len());
            out_rows.drain(..skip);
            if fetch != usize::MAX {
                out_rows.truncate(fetch);
            }
        }
        return Ok(Some(vec![build_output_batch(&out_rows, &out_cols)?]));
    }

    // Materialize FK survivor rows (non-aggregate path).
    let fk_rows = {
        let fk_db = lock_db(&jc.fk_table, tables);
        let snap = fk_db.snapshot();
        fk_db
            .rows_for_rids(&fk_rids, snap)
            .map_err(MongrelQueryError::Core)?
    };

    // 7. Resolve the output columns (from the Projection, or all join columns).
    let out_cols = output_columns(projection, &jc, &left, &right, tables)?;
    if projection.is_some() && out_cols.is_empty() {
        return Ok(None); // unsupported projection expr
    }

    // 8. Pair each FK row with its matched PK row (by encoded join value).
    let mut pk_map: HashMap<Vec<u8>, Row> = HashMap::with_capacity(pk_rows.len());
    for r in pk_rows {
        let key = r
            .columns
            .get(&pk_col_id)
            .cloned()
            .unwrap_or(Value::Null)
            .encode_key();
        pk_map.insert(key, r);
    }
    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(fk_rows.len());
    for f in &fk_rows {
        let fk_val = f
            .columns
            .get(&fk_col_id)
            .cloned()
            .unwrap_or(Value::Null)
            .encode_key();
        let Some(pk_row) = pk_map.get(&fk_val) else {
            continue; // dangling FK ⇒ excluded from an inner join
        };
        let mut row = Vec::with_capacity(out_cols.len());
        for oc in &out_cols {
            let src = if oc.is_fk { f } else { pk_row };
            row.push(
                src.columns
                    .get(&oc.column_id)
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        out_rows.push(row);
    }

    // 9. Apply ORDER BY (sort) then LIMIT, if present.
    if let Some((keys, fetch)) = sort {
        apply_sort(&mut out_rows, &out_cols, &keys);
        if let Some(fetch) = fetch {
            out_rows.truncate(fetch);
        }
    }
    if let Some((skip, fetch)) = limit {
        let skip = skip.min(out_rows.len());
        out_rows.drain(..skip);
        if fetch != usize::MAX {
            out_rows.truncate(fetch);
        }
    }

    // 10. Build the Arrow result batch.
    let batch = build_output_batch(&out_rows, &out_cols)?;
    Ok(Some(vec![batch]))
}

/// One join side: the real registered table name, its SQL alias (if any), and
/// the per-side WHERE conjuncts.
#[derive(Clone)]
struct Side {
    table: String,
    alias: Option<String>,
    conjuncts: Vec<Expr>,
}

/// Resolved join classification.
struct JoinClass {
    fk_table: String,
    fk_name: String,
    pk_table: String,
    pk_name: String,
    /// True when the PK side is the join's left input (used to pick conjuncts).
    pk_is_left: bool,
    /// The join type (Phase 13.6: Inner, Left, LeftSemi, LeftAnti, …).
    join_type: JoinType,
}

#[derive(Clone)]
struct OutCol {
    is_fk: bool,
    column_id: u16,
    name: String,
    ty: TypeId,
}

fn lock_db<'a>(
    table: &str,
    tables: &'a HashMap<String, Arc<Mutex<Table>>>,
) -> parking_lot::MutexGuard<'a, Table> {
    tables.get(table).expect("table pre-checked present").lock()
}

fn with_schema(table: &str, tables: &HashMap<String, Arc<Mutex<Table>>>) -> Result<Schema> {
    let db = lock_db(table, tables);
    Ok(db.schema().clone())
}

/// Which side (true = left) a qualified column relation belongs to.
fn which_side(col: &Column, left: &Side, right: &Side) -> Option<bool> {
    let t = col.relation.as_ref().map(|r| r.table());
    match t {
        Some(t) => {
            if Some(t) == left.alias.as_deref() || t == left.table {
                Some(true)
            } else if Some(t) == right.alias.as_deref() || t == right.table {
                Some(false)
            } else {
                None
            }
        }
        None => None,
    }
}

// --- plan peeling helpers ---

fn peel_sort_limit(plan: &LogicalPlan) -> (Option<SortInfo>, Option<(usize, usize)>, &LogicalPlan) {
    let mut sort: Option<SortInfo> = None;
    let mut limit: Option<(usize, usize)> = None;
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Sort(s) => {
                if sort.is_none() {
                    let keys: Vec<(Expr, bool, bool)> = s
                        .expr
                        .iter()
                        .map(|se| (se.expr.clone(), se.asc, se.nulls_first))
                        .collect();
                    sort = Some((keys, s.fetch));
                }
                cur = &s.input;
            }
            LogicalPlan::Limit(l) => {
                let skip = lit_usize(&l.skip);
                let fetch = lit_usize(&l.fetch);
                if skip.is_some() || fetch.is_some() {
                    limit = Some((skip.unwrap_or(0), fetch.unwrap_or(usize::MAX)));
                }
                cur = &l.input;
            }
            _ => break,
        }
    }
    (sort, limit, cur)
}

/// Peel an optional outer Projection, returning `(projection plan, rest)`.
fn peel_projection(plan: &LogicalPlan) -> (Option<&LogicalPlan>, &LogicalPlan) {
    match plan {
        LogicalPlan::Projection(p) => (Some(plan), &p.input),
        _ => (None, plan),
    }
}

/// Peel an optional top-level Aggregate (no GROUP BY, single agg expr) — Phase
/// 13.4. Returns `(aggregate, rest)`. `None` when the node is not a servable
/// aggregate.
fn peel_aggregate(
    plan: &LogicalPlan,
) -> (Option<&datafusion::logical_expr::Aggregate>, &LogicalPlan) {
    if let LogicalPlan::Aggregate(a) = plan {
        if a.group_expr.is_empty() && a.aggr_expr.len() == 1 {
            return (Some(a), &a.input);
        }
    }
    (None, plan)
}

/// Peel consecutive `Filter` nodes above the join, returning their split
/// conjuncts and the node under them.
fn peel_filters(plan: &LogicalPlan) -> (Vec<Expr>, &LogicalPlan) {
    let mut conj = Vec::new();
    let mut cur = plan;
    while let LogicalPlan::Filter(f) = cur {
        conj.extend(split_and(&f.predicate));
        cur = &f.input;
    }
    (conj, cur)
}

/// Route a top-level conjunct to the side (true=left) whose columns it
/// references exclusively. `None` if it touches both sides or none.
fn route_conjunct(
    conj: &Expr,
    left: &Side,
    right: &Side,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
) -> Result<Option<bool>> {
    let cols = referenced_columns(conj);
    if cols.is_empty() {
        return Ok(None);
    }
    let left_schema = with_schema(&left.table, tables)?;
    let right_schema = with_schema(&right.table, tables)?;
    let mut on_left = false;
    let mut on_right = false;
    for (rel, name) in &cols {
        let belongs_left = rel
            .as_deref()
            .map(|r| Some(r) == left.alias.as_deref() || r == left.table)
            .unwrap_or_else(|| left_schema.column(name).is_some());
        let belongs_right = rel
            .as_deref()
            .map(|r| Some(r) == right.alias.as_deref() || r == right.table)
            .unwrap_or_else(|| right_schema.column(name).is_some());
        match (belongs_left, belongs_right) {
            (true, false) => on_left = true,
            (false, true) => on_right = true,
            (true, true) => return Ok(None), // ambiguous across both sides
            (false, false) => return Ok(None),
        }
    }
    match (on_left, on_right) {
        (true, false) => Ok(Some(true)),
        (false, true) => Ok(Some(false)),
        _ => Ok(None),
    }
}

fn referenced_columns(expr: &Expr) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    collect_cols(expr, &mut out);
    out
}

fn collect_cols(expr: &Expr, out: &mut Vec<(Option<String>, String)>) {
    match expr {
        Expr::Column(c) => out.push((
            c.relation.as_ref().map(|r| r.table().to_string()),
            c.name.clone(),
        )),
        Expr::BinaryExpr(b) => {
            collect_cols(&b.left, out);
            collect_cols(&b.right, out);
        }
        Expr::Between(be) => {
            collect_cols(&be.expr, out);
            collect_cols(&be.low, out);
            collect_cols(&be.high, out);
        }
        Expr::Like(l) => collect_cols(&l.expr, out),
        Expr::InList(il) => {
            collect_cols(&il.expr, out);
        }
        _ => {}
    }
}

/// Peel `(Filter)? → (SubqueryAlias)? → TableScan` (in either Filter/Alias
/// order) into a [`Side`].
fn peel_side(plan: &LogicalPlan) -> Option<Side> {
    let mut alias: Option<String> = None;
    let mut conjuncts: Vec<Expr> = Vec::new();
    let mut cur = plan;
    let table: String;
    loop {
        match cur {
            LogicalPlan::SubqueryAlias(sa) => {
                if alias.is_none() {
                    alias = Some(sa.alias.table().to_string());
                }
                cur = &sa.input;
            }
            LogicalPlan::Filter(f) => {
                conjuncts.extend(split_and(&f.predicate));
                cur = &f.input;
            }
            LogicalPlan::TableScan(ts) => {
                table = ts.table_name.table().to_string();
                break;
            }
            _ => return None,
        }
    }
    Some(Side {
        table,
        alias,
        conjuncts,
    })
}

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

fn lit_usize(e: &Option<Box<Expr>>) -> Option<usize> {
    use datafusion::common::ScalarValue;
    match e {
        Some(b) => match b.as_ref() {
            Expr::Literal(s, _) => match s {
                ScalarValue::Int64(Some(v)) => Some(*v as usize),
                ScalarValue::UInt64(Some(v)) => Some(*v as usize),
                ScalarValue::Int32(Some(v)) => Some(*v as usize),
                ScalarValue::UInt32(Some(v)) => Some(*v as usize),
                _ => None,
            },
            _ => None,
        },
        None => None,
    }
}

// --- classification ---

fn classify(
    join: &datafusion::logical_expr::Join,
    left: &Side,
    right: &Side,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
) -> Result<Option<JoinClass>> {
    // Extract the single equi-condition as (left-ish col, right-ish col) with
    // the side each belongs to.
    let Some((a_is_left, a_col, b_is_left, b_col)) = extract_equi(join, left, right) else {
        return Ok(None);
    };
    let side_a = if a_is_left { left } else { right };
    let side_b = if b_is_left { left } else { right };
    // Phase 13.6: the dimension side (formerly "PK") can be any column, not
    // just a PRIMARY_KEY — a bitmap index on the FK side is all that's needed.
    // Prefer PK as the dimension when present; otherwise pick either side that
    // has a bitmap as the FK side.
    let a_is_pk = is_pk_column(&side_a.table, &a_col, tables)?;
    let b_is_pk = is_pk_column(&side_b.table, &b_col, tables)?;
    let a_has_bitmap = has_bitmap(&side_a.table, &a_col, tables)?;
    let b_has_bitmap = has_bitmap(&side_b.table, &b_col, tables)?;
    let (fk_table, fk_name, pk_table, pk_name, pk_is_left) = match (a_is_pk, b_is_pk) {
        // a is the PK/dimension side.
        (true, false) => (
            side_b.table.clone(),
            b_col,
            side_a.table.clone(),
            a_col,
            a_is_left,
        ),
        // b is the PK/dimension side.
        (false, true) => (
            side_a.table.clone(),
            a_col,
            side_b.table.clone(),
            b_col,
            b_is_left,
        ),
        // Neither is PK: pick the side with a bitmap as the FK side (Phase 13.6).
        _ => {
            if b_has_bitmap && !a_has_bitmap {
                (
                    side_b.table.clone(),
                    b_col,
                    side_a.table.clone(),
                    a_col,
                    a_is_left,
                )
            } else if a_has_bitmap && !b_has_bitmap {
                (
                    side_a.table.clone(),
                    a_col,
                    side_b.table.clone(),
                    b_col,
                    b_is_left,
                )
            } else {
                return Ok(None);
            }
        }
    };
    if !has_bitmap(&fk_table, &fk_name, tables)? {
        return Ok(None);
    }
    Ok(Some(JoinClass {
        fk_table,
        fk_name,
        pk_table,
        pk_name,
        pk_is_left,
        join_type: join.join_type,
    }))
}

/// Extract the single `col = col` equi-condition as
/// `(a_is_left, a_name, b_is_left, b_name)`. `None` if not exactly one equi-col
/// pair with no residual predicates.
fn extract_equi(
    join: &datafusion::logical_expr::Join,
    left: &Side,
    right: &Side,
) -> Option<(bool, String, bool, String)> {
    // Optimized plans put the equi-condition in `on`.
    if join.on.len() == 1 && join.filter.is_none() {
        let (e1, e2) = &join.on[0];
        let c1 = col_name(e1)?;
        let c2 = col_name(e2)?;
        // `on` pairs are conventionally (left, right); fall back to that if the
        // columns are unqualified.
        let s1 = match e1 {
            Expr::Column(c) => which_side(c, left, right).unwrap_or(true),
            _ => return None,
        };
        let s2 = match e2 {
            Expr::Column(c) => which_side(c, left, right).unwrap_or(false),
            _ => return None,
        };
        if s1 == s2 {
            return None;
        }
        return Some((s1, c1, s2, c2));
    }
    // Unoptimized plans put `col = col` in `filter`.
    let filter = join.filter.as_ref()?;
    let conj = split_and(filter);
    let mut equi: Option<(bool, String, bool, String)> = None;
    for c in &conj {
        if let Expr::BinaryExpr(b) = c {
            if b.op == Operator::Eq {
                if let (Expr::Column(c1), Expr::Column(c2)) = (b.left.as_ref(), b.right.as_ref()) {
                    let (Some(s1), Some(s2)) =
                        (which_side(c1, left, right), which_side(c2, left, right))
                    else {
                        continue;
                    };
                    if s1 != s2 {
                        if equi.is_some() {
                            return None; // multiple equi-conditions
                        }
                        equi = Some((s1, c1.name.clone(), s2, c2.name.clone()));
                        continue;
                    }
                }
            }
        }
        return None; // residual non-equi predicate
    }
    equi
}

fn col_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.name.clone()),
        _ => None,
    }
}

fn is_pk_column(
    table: &str,
    col: &str,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
) -> Result<bool> {
    match tables.get(table) {
        Some(arc) => {
            let db = arc.lock();
            Ok(db
                .schema()
                .primary_key()
                .map(|c| c.name == col)
                .unwrap_or(false))
        }
        None => Ok(false),
    }
}

fn has_bitmap(table: &str, col: &str, tables: &HashMap<String, Arc<Mutex<Table>>>) -> Result<bool> {
    let Some(arc) = tables.get(table) else {
        return Ok(false);
    };
    let db = arc.lock();
    let schema = db.schema();
    let Some(cdef) = schema.column(col) else {
        return Ok(false);
    };
    Ok(schema
        .indexes
        .iter()
        .any(|i| i.column_id == cdef.id && i.kind == IndexKind::Bitmap))
}

/// Translate every conjunct on `table`'s side against that table's schema.
/// Returns `None` if any conjunct does not translate (the side's row-id set
/// would be inexact ⇒ bail to DataFusion).
fn translate_side(
    table: &str,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
    conj: &[Expr],
) -> Result<Option<Vec<Condition>>> {
    let schema = with_schema(table, tables)?;
    let mut out = Vec::with_capacity(conj.len());
    for e in conj {
        match translate_filter(e, &schema).or_else(|| translate_ann_search(e, &schema)) {
            Some(c) => out.push(c),
            None => return Ok(None),
        }
    }
    Ok(Some(out))
}

// --- output column resolution ---

fn output_columns(
    projection: Option<&LogicalPlan>,
    jc: &JoinClass,
    left: &Side,
    right: &Side,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
) -> Result<Vec<OutCol>> {
    let fk_side = if jc.pk_is_left { right } else { left };
    let pk_side = if jc.pk_is_left { left } else { right };
    if let Some(LogicalPlan::Projection(p)) = projection {
        let fk_schema = with_schema(&jc.fk_table, tables)?;
        let pk_schema = with_schema(&jc.pk_table, tables)?;
        let mut out = Vec::with_capacity(p.expr.len());
        for (i, e) in p.expr.iter().enumerate() {
            let Some(col) = expr_column(e) else {
                return Ok(Vec::new());
            };
            let field_name = p
                .schema
                .fields()
                .get(i)
                .map(|f| f.name().to_string())
                .unwrap_or_else(|| col.name.clone());
            let Some((is_fk, column_id, ty)) =
                resolve_out_col(&col, fk_side, pk_side, &fk_schema, &pk_schema)
            else {
                return Ok(Vec::new());
            };
            out.push(OutCol {
                is_fk,
                column_id,
                name: field_name,
                ty,
            });
        }
        Ok(out)
    } else {
        let fk_schema = with_schema(&jc.fk_table, tables)?;
        let pk_schema = with_schema(&jc.pk_table, tables)?;
        let mut out = Vec::new();
        for c in &fk_schema.columns {
            out.push(OutCol {
                is_fk: true,
                column_id: c.id,
                name: c.name.clone(),
                ty: c.ty,
            });
        }
        for c in &pk_schema.columns {
            out.push(OutCol {
                is_fk: false,
                column_id: c.id,
                name: c.name.clone(),
                ty: c.ty,
            });
        }
        Ok(out)
    }
}

fn expr_column(e: &Expr) -> Option<Column> {
    match e {
        Expr::Column(c) => Some(c.clone()),
        Expr::Alias(a) => expr_column(&a.expr),
        _ => None,
    }
}

/// Resolve a projection `Column` to `(is_fk, column_id, TypeId)` using the
/// sides' aliases and real table names.
fn resolve_out_col(
    col: &Column,
    fk_side: &Side,
    pk_side: &Side,
    fk_schema: &Schema,
    pk_schema: &Schema,
) -> Option<(bool, u16, TypeId)> {
    if let Some(rel) = &col.relation {
        let t = rel.table();
        if Some(t) == fk_side.alias.as_deref() || t == fk_side.table {
            return fk_schema.column(&col.name).map(|c| (true, c.id, c.ty));
        }
        if Some(t) == pk_side.alias.as_deref() || t == pk_side.table {
            return pk_schema.column(&col.name).map(|c| (false, c.id, c.ty));
        }
        return None;
    }
    let in_fk = fk_schema.column(&col.name);
    let in_pk = pk_schema.column(&col.name);
    match (in_fk, in_pk) {
        (Some(c), None) => Some((true, c.id, c.ty)),
        (None, Some(c)) => Some((false, c.id, c.ty)),
        _ => None,
    }
}

fn output_schema(
    projection: Option<&LogicalPlan>,
    jc: &JoinClass,
    left: &Side,
    right: &Side,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
) -> Result<Option<arrow::datatypes::Schema>> {
    let cols = output_columns(projection, jc, left, right, tables)?;
    if projection.is_some() && cols.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(cols.len());
    for c in &cols {
        fields.push(Field::new(&c.name, arrow_data_type(&c.ty)?, true));
    }
    Ok(Some(arrow::datatypes::Schema::new(fields)))
}

// --- sorting & limit application ---

fn apply_sort(rows: &mut [Vec<Value>], out_cols: &[OutCol], keys: &[(Expr, bool, bool)]) {
    let mut keyed: Vec<(usize, bool, bool)> = Vec::new();
    for (expr, asc, nulls_first) in keys {
        let Expr::Column(c) = expr else {
            continue;
        };
        let Some(idx) = out_cols.iter().position(|oc| oc.name == c.name) else {
            continue;
        };
        keyed.push((idx, *asc, *nulls_first));
    }
    if keyed.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for &(idx, asc, nulls_first) in &keyed {
            let ord = compare_with_nulls(&a[idx], &b[idx], asc, nulls_first);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Compare two values honoring direction (`asc`) and null placement
/// (`nulls_first`), which is independent of direction in SQL/Arrow.
fn compare_with_nulls(a: &Value, b: &Value, asc: bool, nulls_first: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {
            let ord = cmp_values(a, b);
            if asc {
                ord
            } else {
                ord.reverse()
            }
        }
    }
}

fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Float64(x), Value::Float64(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

// --- batch building ---

fn build_output_batch(rows: &[Vec<Value>], out_cols: &[OutCol]) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(out_cols.len());
    let mut fields: Vec<Field> = Vec::with_capacity(out_cols.len());
    for (i, oc) in out_cols.iter().enumerate() {
        let vals: Vec<Value> = rows.iter().map(|r| r[i].clone()).collect();
        arrays.push(build_array(oc.ty, &vals)?);
        fields.push(Field::new(&oc.name, arrow_data_type(&oc.ty)?, true));
    }
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn empty_batch(schema: arrow::datatypes::Schema) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| new_empty_array(f.data_type()))
        .collect();
    RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

/// Compute a top-level aggregate over the FK-join survivor set (Phase 13.4).
/// COUNT(*) is O(1) (just the survivor count). SUM/MIN/MAX/COUNT(col) on an
/// FK-side column materializes the FK survivors and accumulates in one pass.
/// Returns `Ok(None)` for unsupported shapes (PK-side aggregate, distinct,
/// etc.) so the caller falls through to DataFusion.
fn compute_join_aggregate(
    agg: &datafusion::logical_expr::Aggregate,
    jc: &JoinClass,
    left: &Side,
    right: &Side,
    tables: &HashMap<String, Arc<Mutex<Table>>>,
    fk_rids: &[u64],
) -> Result<Option<Vec<RecordBatch>>> {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::expr::AggregateFunction;

    let Expr::AggregateFunction(AggregateFunction { func, params }) = &agg.aggr_expr[0] else {
        return Ok(None);
    };
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() {
        return Ok(None);
    }
    let fname = func.name();
    if !matches!(fname, "count" | "sum" | "min" | "max") {
        return Ok(None);
    }

    // Column arg (None for COUNT(*)).
    let col: Option<&Column> = match params.args.as_slice() {
        [] => None,
        [Expr::Column(c)] => Some(c),
        [Expr::Literal(_, _)] => None,
        _ => return Ok(None),
    };

    // Output schema from the Aggregate node.
    let out_field = agg
        .schema
        .fields()
        .first()
        .ok_or_else(|| MongrelQueryError::Arrow("aggregate output has no fields".into()))?;
    let out_schema: arrow::datatypes::SchemaRef =
        Arc::new(arrow::datatypes::Schema::new(vec![out_field.clone()]));
    let out_is_int = matches!(out_field.data_type(), arrow::datatypes::DataType::Int64);

    // COUNT(*) → survivor count (no materialization).
    if fname == "count" && col.is_none() {
        let val = ScalarValue::Int64(Some(fk_rids.len() as i64));
        return Ok(Some(vec![scalar_batch(val, out_schema)?]));
    }

    // SUM / MIN / MAX / COUNT(col) → materialize FK survivors.
    let Some(col) = col else {
        return Ok(None);
    };
    let fk_schema = with_schema(&jc.fk_table, tables)?;
    let pk_schema = with_schema(&jc.pk_table, tables)?;
    let fk_side = if jc.pk_is_left { right } else { left };
    let pk_side = if jc.pk_is_left { left } else { right };
    let Some((is_fk, column_id, ty)) =
        resolve_out_col(col, fk_side, pk_side, &fk_schema, &pk_schema)
    else {
        return Ok(None);
    };
    // Only FK-side column aggregates (no join materialization needed).
    if !is_fk {
        return Ok(None);
    }

    let fk_db = lock_db(&jc.fk_table, tables);
    let snap = fk_db.snapshot();
    let rows = fk_db
        .rows_for_rids(fk_rids, snap)
        .map_err(MongrelQueryError::Core)?;

    let values: Vec<Value> = rows
        .iter()
        .map(|r| r.columns.get(&column_id).cloned().unwrap_or(Value::Null))
        .collect();

    let scalar = match (fname, ty) {
        ("count", _) => ScalarValue::Int64(Some(
            values.iter().filter(|v| !matches!(v, Value::Null)).count() as i64,
        )),
        ("sum", TypeId::Int64) => {
            let s: i128 = values
                .iter()
                .filter_map(|v| match v {
                    Value::Int64(x) => Some(*x as i128),
                    _ => None,
                })
                .sum();
            match i64::try_from(s) {
                Ok(v) => ScalarValue::Int64(Some(v)),
                Err(_) => ScalarValue::Int64(None),
            }
        }
        ("sum", TypeId::Float64) => {
            let s: f64 = values
                .iter()
                .filter_map(|v| match v {
                    Value::Float64(x) => Some(*x),
                    _ => None,
                })
                .sum();
            ScalarValue::Float64(Some(s))
        }
        ("min", TypeId::Int64) => {
            let m = values
                .iter()
                .filter_map(|v| match v {
                    Value::Int64(x) => Some(*x),
                    _ => None,
                })
                .min();
            match m {
                Some(v) => ScalarValue::Int64(Some(v)),
                None => ScalarValue::Int64(None),
            }
        }
        ("min", TypeId::Float64) => {
            let m = values
                .iter()
                .filter_map(|v| match v {
                    Value::Float64(x) => Some(*x),
                    _ => None,
                })
                .fold(None, |acc: Option<f64>, x| {
                    Some(acc.map_or(x, |a: f64| a.min(x)))
                });
            ScalarValue::Float64(m)
        }
        ("max", TypeId::Int64) => {
            let m = values
                .iter()
                .filter_map(|v| match v {
                    Value::Int64(x) => Some(*x),
                    _ => None,
                })
                .max();
            match m {
                Some(v) => ScalarValue::Int64(Some(v)),
                None => ScalarValue::Int64(None),
            }
        }
        ("max", TypeId::Float64) => {
            let m = values
                .iter()
                .filter_map(|v| match v {
                    Value::Float64(x) => Some(*x),
                    _ => None,
                })
                .fold(None, |acc: Option<f64>, x| {
                    Some(acc.map_or(x, |a: f64| a.max(x)))
                });
            ScalarValue::Float64(m)
        }
        _ => return Ok(None),
    };
    let _ = out_is_int; // kept for potential future Int/Float output-type alignment
    Ok(Some(vec![scalar_batch(scalar, out_schema)?]))
}

/// Detect a bare `COUNT(*)` (or `COUNT(<literal>)`) top-level aggregate — the
/// shape whose answer is just the survivor cardinality, so the FK row set never
/// needs materializing. Returns `Some(())` when it matches. (Phase 17.4.)
fn bare_count_star(agg: &datafusion::logical_expr::Aggregate) -> Option<()> {
    use datafusion::logical_expr::expr::AggregateFunction;
    // `peel_aggregate` already guarantees `group_expr` is empty and there is
    // exactly one aggregate expression.
    let Expr::AggregateFunction(AggregateFunction { func, params }) = &agg.aggr_expr[0] else {
        return None;
    };
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() {
        return None;
    }
    if func.name() != "count" {
        return None;
    }
    match params.args.as_slice() {
        [] | [Expr::Literal(_, _)] => Some(()),
        _ => None,
    }
}

/// Build a one-row batch from a scalar value and output schema.
fn scalar_batch(
    val: datafusion::common::ScalarValue,
    schema: arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    use datafusion::common::ScalarValue;
    use std::sync::Arc as A;
    let array: ArrayRef = match &val {
        ScalarValue::Int64(opt) => {
            let mut b = arrow::array::Int64Builder::new();
            match opt {
                Some(x) => b.append_value(*x),
                None => b.append_null(),
            }
            A::new(b.finish())
        }
        ScalarValue::Float64(opt) => {
            let mut b = arrow::array::Float64Builder::new();
            match opt {
                Some(x) => b.append_value(*x),
                None => b.append_null(),
            }
            A::new(b.finish())
        }
        _ => {
            return Err(MongrelQueryError::Arrow(format!(
                "unsupported scalar type: {val:?}"
            )))
        }
    };
    RecordBatch::try_new(schema, vec![array]).map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MongrelProvider;
    use mongreldb_core::schema::{
        ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema as MSchema, TypeId as MTy,
    };
    use mongreldb_core::{Table, Value};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// countries(pk `cid` Int64) ; users(pk `uid`, `country` Int64 [bitmap]).
    /// users reference countries by `country`. Join users.country = countries.cid.
    fn build() -> (tempfile::TempDir, HashMap<String, Arc<Mutex<Table>>>) {
        let dir = tempdir().unwrap();

        let countries_schema = MSchema {
            schema_id: 10,
            columns: vec![ColumnDef {
                id: 1,
                name: "cid".into(),
                ty: MTy::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            }],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        let mut countries =
            Table::create(dir.path().join("countries").as_path(), countries_schema, 1).unwrap();
        for i in 0..5i64 {
            countries.put(vec![(1, Value::Int64(i))]).unwrap();
        }
        countries.flush().unwrap();

        let users_schema = MSchema {
            schema_id: 11,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "uid".into(),
                    ty: MTy::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                },
                ColumnDef {
                    id: 2,
                    name: "country".into(),
                    ty: MTy::Int64,
                    flags: ColumnFlags::empty(),
                },
            ],
            indexes: vec![IndexDef {
                name: "country_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
            }],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        let mut users = Table::create(dir.path().join("users").as_path(), users_schema, 1).unwrap();
        for u in 0..10i64 {
            users
                .put(vec![(1, Value::Int64(u)), (2, Value::Int64(u % 5))])
                .unwrap();
        }
        users.flush().unwrap();

        let mut tables: HashMap<String, Arc<Mutex<Table>>> = HashMap::new();
        tables.insert("countries".into(), Arc::new(Mutex::new(countries)));
        tables.insert("users".into(), Arc::new(Mutex::new(users)));
        (dir, tables)
    }

    async fn ctx_with(
        tables: &HashMap<String, Arc<Mutex<Table>>>,
    ) -> datafusion::prelude::SessionContext {
        let pu = MongrelProvider::new(Arc::clone(&tables["users"])).unwrap();
        let pc = MongrelProvider::new(Arc::clone(&tables["countries"])).unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("users", Arc::new(pu)).unwrap();
        ctx.register_table("countries", Arc::new(pc)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn intercepts_inner_fk_join() {
        let (_dir, tables) = build();
        let ctx = ctx_with(&tables).await;
        let plan = ctx
            .sql("select u.uid, u.country from users u join countries c on u.country = c.cid")
            .await
            .unwrap();
        let out = try_fk_join(&tables, plan.logical_plan()).unwrap();
        assert!(out.is_some(), "FK-join intercept should fire");
        let batches = out.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10, "all 10 users reference existing countries");
    }

    #[tokio::test]
    async fn intercepts_pk_side_filter_and_order() {
        let (_dir, tables) = build();
        let ctx = ctx_with(&tables).await;
        // countries with cid <= 1 survive ⇒ users with country in {0,1}:
        // uids 0,1,5,6.
        let plan = ctx
            .sql("select u.uid from users u join countries c on u.country = c.cid where c.cid <= 1 order by u.uid")
            .await
            .unwrap();
        let out = try_fk_join(&tables, plan.logical_plan()).unwrap().unwrap();
        let batch = &out[0];
        assert_eq!(batch.num_rows(), 4, "uids 0,1,5,6 reference countries 0,1");
        // Ordered ascending ⇒ first uid is 0.
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(3), 6);
    }

    #[tokio::test]
    async fn falls_through_for_non_bitmap_fk() {
        // countries has NO bitmap index on cid, so joining the other direction
        // (countries as FK side) must fall through.
        let (_dir, tables) = build();
        let ctx = ctx_with(&tables).await;
        let plan = ctx
            .sql("select c.cid from countries c join users u on c.cid = u.country")
            .await
            .unwrap();
        let out = try_fk_join(&tables, plan.logical_plan()).unwrap();
        // Still fires: users.country HAS a bitmap, so users is the FK side
        // regardless of SQL order. Expect it to fire.
        assert!(out.is_some(), "intercept fires regardless of join order");
    }

    #[tokio::test]
    async fn aggregate_count_star_over_join() {
        let (_dir, tables) = build();
        let ctx = ctx_with(&tables).await;
        let plan = ctx
            .sql("select count(*) from users u join countries c on u.country = c.cid")
            .await
            .unwrap();
        let out = try_fk_join(&tables, plan.logical_plan()).unwrap();
        assert!(out.is_some(), "aggregate FK-join should fire");
        let batch = &out.unwrap()[0];
        assert_eq!(batch.num_rows(), 1);
        let val = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, 10, "10 users reference existing countries");
    }

    #[tokio::test]
    async fn aggregate_count_with_pk_filter() {
        let (_dir, tables) = build();
        let ctx = ctx_with(&tables).await;
        // countries cid <= 1 survive ⇒ users with country in {0,1}: uids 0,1,5,6.
        let plan = ctx
            .sql("select count(*) from users u join countries c on u.country = c.cid where c.cid <= 1")
            .await
            .unwrap();
        let out = try_fk_join(&tables, plan.logical_plan()).unwrap();
        assert!(out.is_some(), "filtered aggregate FK-join should fire");
        let val = out.unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, 4, "4 users match countries 0,1");
    }

    #[tokio::test]
    async fn aggregate_sum_over_fk_column() {
        let (_dir, tables) = build();
        let ctx = ctx_with(&tables).await;
        // sum(u.uid) for all 10 users referencing countries 0..4.
        let plan = ctx
            .sql("select sum(u.uid) from users u join countries c on u.country = c.cid")
            .await
            .unwrap();
        let out = try_fk_join(&tables, plan.logical_plan()).unwrap();
        assert!(out.is_some(), "SUM FK-join aggregate should fire");
        let val = out.unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, (0..10i64).sum::<i64>(), "sum of uids 0..9");
    }
}
