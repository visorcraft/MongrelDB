//! `FFITable` — an opaque table handle that re-resolves the underlying engine
//! table on every call (safe against drop/rename, matching the NAPI pattern),
//! plus `FFIResult` for query iteration.
//!
//! Cells cross the ABI as `mongreldb_cell` (a column id + [`CValue`]); rows as
//! `mongreldb_row` (a row id + cell array). A query result holds the materialized
//! `Vec<Row>` plus the table schema so cell expansion can map values back into
//! column order.

use crate::cstr::cstr_to_string;
use crate::database::{as_db, mongreldb_database_t, FFIDatabase};
use crate::error::{clear, set_error, set_error_msg, ErrorCode};
use crate::query::{self, mongreldb_query_t};
use crate::value::{value_to_c, CValue, EmbeddingSlice};
use mongreldb_core::query::{AnnRerankRequest, Query as CoreQuery, VectorMetric};
use mongreldb_core::schema::{Schema, TypeId};
use mongreldb_core::{RowId, Value};
use std::os::raw::c_void;
use std::sync::Arc;

/// Opaque table handle.
pub type mongreldb_table_t = *mut c_void;
/// Opaque result handle.
pub type mongreldb_result_t = *mut c_void;
/// Opaque exact ANN rerank result handle.
pub type mongreldb_ann_rerank_result_t = *mut c_void;

/// Exact vector metric used by [`mongreldb_table_ann_rerank`].
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_vector_metric {
    Cosine = 0,
    DotProduct = 1,
    Euclidean = 2,
}

/// One exact ANN rerank hit.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct mongreldb_ann_rerank_hit {
    pub row_id: u64,
    pub hamming_distance: u32,
    pub exact_score: f32,
}

struct FFIAnnRerankResult {
    hits: Vec<mongreldb_ann_rerank_hit>,
}

/// One cell: a column id + a tagged value.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct mongreldb_cell {
    pub column_id: u16,
    pub value: CValue,
}

/// A borrowed view of a row's cells: pointer + length.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct mongreldb_cell_slice {
    pub data: *const mongreldb_cell,
    pub len: usize,
}

impl Default for mongreldb_cell_slice {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            len: 0,
        }
    }
}

/// One row: a stable physical row id + a cell array.
#[repr(C)]
pub struct mongreldb_row {
    pub row_id: u64,
    pub cells: mongreldb_cell_slice,
}

/// The Rust-side table wrapper. Holds an `Arc<Database>` and the table name;
/// re-resolves the engine table via `db.table(&name)` on each call (safe
/// against drop/rename, matching the NAPI addon).
pub struct FFITable {
    pub db: Arc<mongreldb_core::Database>,
    pub name: String,
}

impl FFITable {
    pub fn new(db: Arc<mongreldb_core::Database>, name: String) -> Self {
        Self { db, name }
    }

    pub fn into_handle(self) -> mongreldb_table_t {
        Box::into_raw(Box::new(self)) as mongreldb_table_t
    }
}

/// SAFETY helper: borrow a table handle as `&FFITable`.
///
/// # Safety
/// `handle` must be null or a valid table handle.
pub(crate) unsafe fn as_table(handle: mongreldb_table_t) -> Option<&'static FFITable> {
    if handle.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "table handle is null");
        return None;
    }
    Some(&*(handle as *const FFITable))
}

/// The Rust-side query result. Holds the materialized rows plus the table
/// schema (for cell expansion) and the backing byte buffers that the output
/// `CValue`s borrow into.
pub struct FFIResult {
    /// Materialized rows, each with cells in schema column order.
    pub rows: Vec<Vec<(u16, Value)>>,
    /// Row ids aligned with `rows`.
    pub row_ids: Vec<u64>,
    /// The schema used to interpret each cell's value type.
    pub schema: Schema,
    /// Backing store for variable-length `CValue` payloads. One buffer per
    /// cell-payload that needed copying. Kept alive for the result's lifetime.
    pub backing: Vec<Vec<u8>>,
    /// Per-row owned `Vec<mongreldb_cell>` boxes returned by
    /// `mongreldb_result_row`. Kept here so the cell pointers stay valid until
    /// the result handle is freed.
    pub row_cell_drops: Vec<Box<Vec<mongreldb_cell>>>,
}

impl FFIResult {
    pub fn into_handle(self) -> mongreldb_result_t {
        Box::into_raw(Box::new(self)) as mongreldb_result_t
    }
}

/// SAFETY helper: borrow a result handle as `&FFIResult`.
///
/// # Safety
/// `handle` must be null or a valid result handle.
unsafe fn as_result(handle: mongreldb_result_t) -> Option<&'static FFIResult> {
    if handle.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "result handle is null");
        return None;
    }
    Some(&*(handle as *const FFIResult))
}

unsafe fn as_ann_rerank_result(
    handle: mongreldb_ann_rerank_result_t,
) -> Option<&'static FFIAnnRerankResult> {
    if handle.is_null() {
        set_error_msg(
            ErrorCode::InvalidArgument,
            "ANN rerank result handle is null",
        );
        return None;
    }
    Some(&*(handle as *const FFIAnnRerankResult))
}

/// Marshal a core row into a `Vec<(u16, Value)>` in schema column order,
/// filling absent cells with `Value::Null`. Mirrors the NAPI `row_to_js_table`.
pub(crate) fn row_to_cells(
    schema: &Schema,
    columns: &std::collections::HashMap<u16, Value>,
) -> Vec<(u16, Value)> {
    schema
        .columns
        .iter()
        .map(|cd| {
            let v = columns.get(&cd.id).cloned().unwrap_or(Value::Null);
            (cd.id, v)
        })
        .collect()
}

// ── table handle lifecycle ────────────────────────────────────────────────

/// Get a handle to a table by name for typed put/query operations. The handle
/// re-resolves the table on each call. Returns null if the table doesn't exist.
///
/// # Safety
/// `db` must be a valid database handle; `name` must be a NUL-terminated UTF-8
/// C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_table(
    db: mongreldb_database_t,
    name: *const std::os::raw::c_char,
) -> mongreldb_table_t {
    clear();
    let Some(h) = as_db(db) else {
        return std::ptr::null_mut();
    };
    if name.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "table name is null");
        return std::ptr::null_mut();
    }
    let name = cstr_to_string(name, "table name");
    // Validate the table exists now.
    if let Err(e) = h.db.table(&name) {
        set_error(&e);
        return std::ptr::null_mut();
    }
    FFITable::new(Arc::clone(&h.db), name).into_handle()
}

/// Free a table handle. No-op on null.
///
/// # Safety
/// `t` must be null or a valid table handle, and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_free(t: mongreldb_table_t) {
    if t.is_null() {
        return;
    }
    drop(Box::from_raw(t as *mut FFITable));
}

// ── put / put_batch ───────────────────────────────────────────────────────

/// One row's worth of cells crossing the ABI: column-id + value pairs.
#[repr(C)]
pub struct mongreldb_cell_input {
    pub column_id: u16,
    pub value: CValue,
}

/// A borrowed array of input cells.
#[repr(C)]
pub struct mongreldb_cell_input_array {
    pub data: *const mongreldb_cell_input,
    pub len: usize,
}

/// A borrowed array of rows (each a cell-input array) for batch puts.
#[repr(C)]
pub struct mongreldb_row_input_array {
    pub data: *const mongreldb_cell_input_array,
    pub len: usize,
}

/// Convert C input cells into `(column_id, Value)` pairs using the table's
/// schema for type interpretation. Mirrors the NAPI `cell_pairs_table`. Shared
/// by the table and transaction modules.
///
/// # Safety
/// `cells.data` must be valid for `cells.len` `mongreldb_cell_input`s, and each
/// cell's `CValue` pointers must be valid.
pub unsafe fn cell_inputs_to_pairs(
    schema: &Schema,
    cells: &mongreldb_cell_input_array,
) -> Result<Vec<(u16, Value)>, ErrorCode> {
    if cells.data.is_null() {
        return Ok(Vec::new());
    }
    let slice = std::slice::from_raw_parts(cells.data, cells.len);
    let mut out = Vec::with_capacity(slice.len());
    for ci in slice {
        let ty = schema
            .columns
            .iter()
            .find(|cd| cd.id == ci.column_id)
            .map(|cd| cd.ty.clone())
            .unwrap_or(TypeId::Bytes);
        // SAFETY: the caller guarantees the CValue's pointers are valid.
        let v = crate::value::c_to_value(&ci.value, &ty)?;
        out.push((ci.column_id, v));
    }
    Ok(out)
}

/// Upsert one row. Returns 0 on success and writes the storage row id into
/// `out_row_id` (if non-null).
///
/// # Safety
/// `t` must be a valid table handle; `cells` must point to a valid cell-input
/// array; `out_row_id` if non-null must be a valid `u64` slot.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_put(
    t: mongreldb_table_t,
    cells: *const mongreldb_cell_input_array,
    out_row_id: *mut u64,
) -> i32 {
    clear();
    let Some(table) = as_table(t) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if cells.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "cells is null").as_return();
    }
    // Get the schema for type-directed value conversion.
    let handle = match table.db.table(&table.name) {
        Ok(h) => h,
        Err(e) => return set_error(&e).as_return(),
    };
    let schema = handle.lock().schema().clone();
    let cols = match cell_inputs_to_pairs(&schema, &*cells) {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    // Route through Database::transaction so the epoch is properly published
    // (Table::put_returning + Table::commit works for private WAL but the
    // shared WAL path needs the Database-level transaction machinery).
    let table_name = table.name.clone();
    match table
        .db
        .transaction_with_row_ids_for_current_principal(|tx| tx.put_returning(&table_name, cols))
    {
        Ok((_put_result, row_ids)) => {
            let Some(row_id) = row_ids.first() else {
                return set_error_msg(ErrorCode::Unknown, "committed put returned no row ID")
                    .as_return();
            };
            if !out_row_id.is_null() {
                *out_row_id = row_id.0;
            }
            0
        }
        Err(e) => set_error(&e).as_return(),
    }
}

/// Insert a batch of rows. Returns 0 on success. Row ids are not returned
/// (batch callers usually don't need them; use `put` for per-row ids).
///
/// # Safety
/// `t` must be valid; `rows` must point to a valid row-input array.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_put_batch(
    t: mongreldb_table_t,
    rows: *const mongreldb_row_input_array,
) -> i32 {
    clear();
    let Some(table) = as_table(t) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if rows.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "rows is null").as_return();
    }
    let rows = &*rows;
    if rows.data.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "rows.data is null").as_return();
    }
    let handle = match table.db.table(&table.name) {
        Ok(h) => h,
        Err(e) => return set_error(&e).as_return(),
    };
    let schema = handle.lock().schema().clone();
    let row_slices = std::slice::from_raw_parts(rows.data, rows.len);
    let mut batch = Vec::with_capacity(row_slices.len());
    for cells in row_slices {
        let cols = match cell_inputs_to_pairs(&schema, cells) {
            Ok(p) => p,
            Err(code) => return code.as_return(),
        };
        batch.push(cols);
    }
    // Route through Database::transaction for proper epoch management.
    let table_name = table.name.clone();
    match table
        .db
        .transaction_for_current_principal(|tx| tx.put_batch(&table_name, batch))
    {
        Ok(_) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

// ── query ─────────────────────────────────────────────────────────────────

/// Run a hybrid index query. Returns a result handle (or null on error). The
/// caller iterates with [`mongreldb_result_count`] / [`mongreldb_result_row`]
/// and frees with [`mongreldb_result_free`].
///
/// # Safety
/// `t` must be a valid table handle; `q` must be a valid query handle. The
/// caller keeps ownership of `q` and must free it.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_query(
    t: mongreldb_table_t,
    q: mongreldb_query_t,
) -> mongreldb_result_t {
    clear();
    let Some(table) = as_table(t) else {
        return std::ptr::null_mut();
    };
    let Some(builder) = query::as_query(q) else {
        set_error_msg(ErrorCode::InvalidArgument, "query handle is null");
        return std::ptr::null_mut();
    };
    let core_query: CoreQuery = builder.build();
    let projection = builder.projection.clone();
    let limit = builder.limit;

    let handle = match table.db.table(&table.name) {
        Ok(h) => h,
        Err(e) => {
            set_error(&e);
            return std::ptr::null_mut();
        }
    };
    let schema = handle.lock().schema().clone();
    let rows =
        match table
            .db
            .query_for_current_principal(&table.name, &core_query, projection.as_deref())
        {
            Ok(r) => r,
            Err(e) => {
                set_error(&e);
                return std::ptr::null_mut();
            }
        };

    // Materialize rows as `(row_id, Vec<(u16, Value)>)` in schema column order.
    // If a projection is set, keep only those columns; otherwise expand to all
    // schema columns.
    let mut out_rows: Vec<Vec<(u16, Value)>> = Vec::with_capacity(rows.len());
    let mut out_ids: Vec<u64> = Vec::with_capacity(rows.len());
    for row in rows.iter().take(limit.unwrap_or(usize::MAX)) {
        let cells = match &projection {
            Some(proj) => proj
                .iter()
                .map(|cid| {
                    let v = row.columns.get(cid).cloned().unwrap_or(Value::Null);
                    (*cid, v)
                })
                .collect(),
            None => row_to_cells(&schema, &row.columns),
        };
        out_rows.push(cells);
        out_ids.push(row.row_id.0);
    }
    let backing: Vec<Vec<u8>> = Vec::new();
    FFIResult {
        rows: out_rows,
        row_ids: out_ids,
        schema,
        backing,
        row_cell_drops: Vec::new(),
    }
    .into_handle()
}

/// Run binary ANN candidate generation followed by exact float-vector reranking.
/// Returns a result handle, or null on error.
///
/// # Safety
/// `t` must be a valid table handle. `query.data` must be valid for `query.len`
/// floats. The returned handle must be freed with
/// [`mongreldb_ann_rerank_result_free`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_ann_rerank(
    t: mongreldb_table_t,
    column_id: u16,
    query: EmbeddingSlice,
    candidate_k: usize,
    limit: usize,
    metric: i32,
) -> mongreldb_ann_rerank_result_t {
    clear();
    let Some(table) = as_table(t) else {
        return std::ptr::null_mut();
    };
    let metric = match metric {
        0 => VectorMetric::Cosine,
        1 => VectorMetric::DotProduct,
        2 => VectorMetric::Euclidean,
        value => {
            set_error_msg(
                ErrorCode::InvalidArgument,
                format!("invalid vector metric {value}; expected 0, 1, or 2"),
            );
            return std::ptr::null_mut();
        }
    };
    let request = AnnRerankRequest {
        column_id,
        query: query.to_vec(),
        candidate_k,
        limit,
        metric,
    };
    let hits = match table
        .db
        .ann_rerank_for_current_principal(&table.name, &request)
    {
        Ok(hits) => hits,
        Err(error) => {
            set_error(&error);
            return std::ptr::null_mut();
        }
    };
    let hits = hits
        .into_iter()
        .map(|hit| mongreldb_ann_rerank_hit {
            row_id: hit.row_id.0,
            hamming_distance: hit.hamming_distance,
            exact_score: hit.exact_score,
        })
        .collect();
    Box::into_raw(Box::new(FFIAnnRerankResult { hits })) as mongreldb_ann_rerank_result_t
}

/// Number of exact ANN rerank hits. Returns 0 on null and sets an error.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_ann_rerank_result_count(
    result: mongreldb_ann_rerank_result_t,
) -> usize {
    clear();
    as_ann_rerank_result(result)
        .map(|result| result.hits.len())
        .unwrap_or_default()
}

/// Copy one exact ANN rerank hit into `out_hit`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_ann_rerank_result_hit(
    result: mongreldb_ann_rerank_result_t,
    index: usize,
    out_hit: *mut mongreldb_ann_rerank_hit,
) -> i32 {
    clear();
    let Some(result) = as_ann_rerank_result(result) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if out_hit.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_hit is null").as_return();
    }
    let Some(hit) = result.hits.get(index) else {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            format!(
                "ANN rerank hit index {index} out of bounds (len {})",
                result.hits.len()
            ),
        )
        .as_return();
    };
    *out_hit = *hit;
    0
}

/// Free an exact ANN rerank result handle. No-op on null.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_ann_rerank_result_free(result: mongreldb_ann_rerank_result_t) {
    if !result.is_null() {
        drop(Box::from_raw(result as *mut FFIAnnRerankResult));
    }
}

/// Live row count (O(1)). Returns 0 on success and writes the count into
/// `out_count`.
///
/// # Safety
/// `t` must be valid; `out_count` must be a valid `u64` slot.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_count(t: mongreldb_table_t, out_count: *mut u64) -> i32 {
    clear();
    let Some(table) = as_table(t) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if out_count.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_count is null").as_return();
    }
    *out_count = match table.db.count_for(&table.name, None) {
        Ok(count) => count,
        Err(error) => return set_error(&error).as_return(),
    };
    0
}

/// Delete a row by storage row id. Returns 0 on success.
///
/// # Safety
/// `t` must be valid.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_delete(t: mongreldb_table_t, row_id: u64) -> i32 {
    clear();
    let Some(table) = as_table(t) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match table.db.transaction_for_current_principal(|transaction| {
        transaction.delete(&table.name, RowId(row_id))
    }) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

// ── result iteration ──────────────────────────────────────────────────────

/// Number of rows in a result handle. Returns 0 on null (and sets an error).
///
/// # Safety
/// `r` must be null or a valid result handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_result_count(r: mongreldb_result_t) -> usize {
    clear();
    let Some(res) = as_result(r) else {
        return 0;
    };
    res.rows.len()
}

/// Read a row out of a result by index. The row's cells are written into a
/// caller-provided `mongreldb_row` struct; variable-length cell values point
/// into memory owned by the result handle (valid until
/// [`mongreldb_result_free`]).
///
/// Returns 0 on success, negative if the index is out of bounds.
///
/// # Safety
/// `r` must be a valid result handle; `index` must be `< result_count`;
/// `out_row` must point to a writable `mongreldb_row`. The caller must not
/// access the cell pointers after freeing the result handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_result_row(
    r: mongreldb_result_t,
    index: usize,
    out_row: *mut mongreldb_row,
) -> i32 {
    clear();
    let Some(res) = as_result(r) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if out_row.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_row is null").as_return();
    }
    if index >= res.rows.len() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            format!("row index {index} out of bounds (len {})", res.rows.len()),
        )
        .as_return();
    }

    // Build the cell array for this row. Each variable-length value is copied
    // into the result's backing store so the pointer stays valid for the
    // result's lifetime.
    let cells = &res.rows[index];
    // We need mutable access to `res.backing` to append buffers, but `as_result`
    // returned a `&'static` borrow. Re-cast through the raw pointer.
    let backing = &mut (*(r as *mut FFIResult)).backing;
    let mut built: Vec<mongreldb_cell> = Vec::with_capacity(cells.len());
    for (cid, v) in cells {
        let cv = value_to_c(v, backing);
        built.push(mongreldb_cell {
            column_id: *cid,
            value: cv,
        });
    }
    // Move the built vec into a Box stashed on the result handle so its storage
    // lives as long as the handle. Hand the caller a pointer into that Box's
    // heap allocation. Capture the pointer/length *before* moving the Vec into
    // the Box so we read from the Vec (not the Box<Vec>).
    let ptr = built.as_ptr();
    let len = built.len();
    let drops = &mut (*(r as *mut FFIResult)).row_cell_drops;
    drops.push(Box::new(built));

    let row_id = res.row_ids[index];
    *out_row = mongreldb_row {
        row_id,
        cells: mongreldb_cell_slice { data: ptr, len },
    };
    0
}

/// Number of cells in a row. Returns 0 on null.
///
/// # Safety
/// `row` must be null or point to a valid `mongreldb_row` previously filled by
/// [`mongreldb_result_row`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_row_cell_count(row: *const mongreldb_row) -> usize {
    if row.is_null() {
        return 0;
    }
    (*row).cells.len
}

/// Read a cell out of a row by index. Copies the cell value (the `CValue`
/// payload is a bitwise copy; variable-length pointers still alias the result
/// handle's backing store).
///
/// Returns 0 on success, negative if the index is out of bounds.
///
/// # Safety
/// `row` must point to a valid `mongreldb_row`; `index` must be `< cell_count`;
/// `out_cell` must point to a writable `mongreldb_cell`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_row_cell(
    row: *const mongreldb_row,
    index: usize,
    out_cell: *mut mongreldb_cell,
) -> i32 {
    clear();
    if row.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "row is null").as_return();
    }
    if out_cell.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_cell is null").as_return();
    }
    let cells = (*row).cells;
    if index >= cells.len {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            format!("cell index {index} out of bounds (len {})", cells.len),
        )
        .as_return();
    }
    // SAFETY: `data` is valid for `len` cells (filled by result_row).
    let cell = &*cells.data.add(index);
    *out_cell = mongreldb_cell {
        column_id: cell.column_id,
        value: cell.value,
    };
    0
}

/// Free a result handle. No-op on null.
///
/// # Safety
/// `r` must be null or a valid result handle, and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_result_free(r: mongreldb_result_t) {
    if r.is_null() {
        return;
    }
    // SAFETY: upheld by caller. Drops the FFIResult (and with it the backing
    // store + per-row cell boxes).
    drop(Box::from_raw(r as *mut FFIResult));
}

// Keep the unused `FFIDatabase` import reachable (used by callers that share
// the database handle type).
#[allow(dead_code)]
fn _ensure_db_type_imported(_: &FFIDatabase) {}
