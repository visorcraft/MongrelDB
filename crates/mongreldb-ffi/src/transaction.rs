//! `FFITransaction` — the staging-buffer transaction pattern (matching the
//! NAPI addon). A transaction holds `Arc<Database>` + `Mutex<Vec<StagedOp>>`.
//! Each staging call (`mongreldb_txn_put`, `…_upsert`, `…_delete`,
//! `…_delete_by_pk`) appends to the buffer; `mongreldb_txn_commit` replays the
//! buffer into a fresh core `Transaction` and commits atomically; `_rollback`
//! discards the buffer.
//!
//! This is necessary because the engine's `core::Transaction<'db>` borrows the
//! database with a lifetime and cannot be exposed across the FFI boundary —
//! so we stage ops in an owned buffer and replay them at commit time.

use crate::cstr::cstr_to_string;
use crate::database::{as_db, mongreldb_database_t};
use crate::error::{clear, set_error, set_error_msg, ErrorCode};
use crate::table::{cell_inputs_to_pairs, mongreldb_cell_input_array};
use crate::value::{c_to_value, ByteSlice};
use mongreldb_core::query::Query;
use mongreldb_core::schema::TypeId;
use mongreldb_core::{Database as CoreDatabase, Epoch, RowId, UpsertAction, Value};
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex};

/// Opaque transaction handle.
pub type mongreldb_transaction_t = *mut c_void;

/// One staged mutation against a named table.
enum StagedOp {
    Put {
        table: String,
        cells: Vec<(u16, Value)>,
    },
    Upsert {
        table: String,
        cells: Vec<(u16, Value)>,
        update_cells: Vec<(u16, Value)>,
    },
    Delete {
        table: String,
        row_id: RowId,
    },
    DeleteByPk {
        table: String,
        pk: Vec<u8>,
    },
}

/// The Rust-side transaction wrapper. Holds a clone of the database `Arc` and
/// a mutex-protected staging buffer.
pub struct FFITransaction {
    pub db: Arc<CoreDatabase>,
    staging: Mutex<Vec<StagedOp>>,
}

impl FFITransaction {
    pub fn new(db: Arc<CoreDatabase>) -> Self {
        Self {
            db,
            staging: Mutex::new(Vec::new()),
        }
    }

    pub fn into_handle(self) -> mongreldb_transaction_t {
        Box::into_raw(Box::new(self)) as mongreldb_transaction_t
    }
}

/// SAFETY helper: borrow a transaction handle.
///
/// # Safety
/// `txn` must be null or a valid transaction handle.
unsafe fn as_txn(txn: mongreldb_transaction_t) -> Option<&'static FFITransaction> {
    if txn.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "transaction handle is null");
        return None;
    }
    Some(&*(txn as *const FFITransaction))
}

/// Look up a table's schema (cloned) so we can interpret input cell types.
fn table_schema(
    db: &CoreDatabase,
    table: &str,
) -> Result<mongreldb_core::schema::Schema, ErrorCode> {
    let handle = db.table(table).map_err(|e| set_error(&e))?;
    let g = handle.lock();
    Ok(g.schema().clone())
}

// ── lifecycle ─────────────────────────────────────────────────────────────

/// Begin a new cross-table transaction. Returns a handle or null on error.
///
/// # Safety
/// `db` must be a valid database handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_begin(db: mongreldb_database_t) -> mongreldb_transaction_t {
    clear();
    let Some(h) = as_db(db) else {
        return std::ptr::null_mut();
    };
    FFITransaction::new(Arc::clone(&h.db)).into_handle()
}

/// Free a transaction handle (discards any uncommitted staged ops). No-op on
/// null. Does not roll back — staged ops are simply dropped (nothing was made
/// durable).
///
/// # Safety
/// `txn` must be null or a valid transaction handle, and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_free(txn: mongreldb_transaction_t) {
    if txn.is_null() {
        return;
    }
    drop(Box::from_raw(txn as *mut FFITransaction));
}

/// Discard all staged ops. Returns 0.
///
/// # Safety
/// `txn` must be a valid transaction handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_rollback(txn: mongreldb_transaction_t) -> i32 {
    clear();
    let Some(t) = as_txn(txn) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if let Ok(mut staging) = t.staging.lock() {
        staging.clear();
    }
    0
}

// ── staging ───────────────────────────────────────────────────────────────

/// Stage a put on `table`. Returns 0 on success.
///
/// # Safety
/// `txn` must be valid; `table` must be a NUL-terminated UTF-8 C string;
/// `cells` must point to a valid cell-input array.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_put(
    txn: mongreldb_transaction_t,
    table: *const c_char,
    cells: *const mongreldb_cell_input_array,
) -> i32 {
    clear();
    let Some(t) = as_txn(txn) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if table.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "table is null").as_return();
    }
    if cells.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "cells is null").as_return();
    }
    let table_name = cstr_to_string(table, "table name");
    let schema = match table_schema(&t.db, &table_name) {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    let cols = match cell_inputs_to_pairs(&schema, &*cells) {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    if let Ok(mut staging) = t.staging.lock() {
        staging.push(StagedOp::Put {
            table: table_name,
            cells: cols,
        });
    }
    0
}

/// Stage an upsert on `table` with separate insert and update cell sets.
/// `update_cells` may be null (treated as `DO NOTHING`).
///
/// # Safety
/// `txn` must be valid; `table` and `cells` non-null; `update_cells` may be
/// null.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_upsert(
    txn: mongreldb_transaction_t,
    table: *const c_char,
    cells: *const mongreldb_cell_input_array,
    update_cells: *const mongreldb_cell_input_array,
) -> i32 {
    clear();
    let Some(t) = as_txn(txn) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if table.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "table is null").as_return();
    }
    if cells.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "cells is null").as_return();
    }
    let table_name = cstr_to_string(table, "table name");
    let schema = match table_schema(&t.db, &table_name) {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    let insert_cols = match cell_inputs_to_pairs(&schema, &*cells) {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    let update_cols = if update_cells.is_null() {
        Vec::new()
    } else {
        match cell_inputs_to_pairs(&schema, &*update_cells) {
            Ok(p) => p,
            Err(code) => return code.as_return(),
        }
    };
    if let Ok(mut staging) = t.staging.lock() {
        staging.push(StagedOp::Upsert {
            table: table_name,
            cells: insert_cols,
            update_cells: update_cols,
        });
    }
    0
}

/// Stage a delete of `row_id` on `table`. Returns 0 on success.
///
/// # Safety
/// `txn` must be valid; `table` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_delete(
    txn: mongreldb_transaction_t,
    table: *const c_char,
    row_id: u64,
) -> i32 {
    clear();
    let Some(t) = as_txn(txn) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if table.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "table is null").as_return();
    }
    let table_name = cstr_to_string(table, "table name");
    // Validate the table exists.
    if let Err(e) = t.db.table(&table_name) {
        return set_error(&e).as_return();
    }
    if let Ok(mut staging) = t.staging.lock() {
        staging.push(StagedOp::Delete {
            table: table_name,
            row_id: RowId(row_id),
        });
    }
    0
}

/// Stage a delete of the first row matching a primary key on `table`. The PK
/// bytes are taken from `pk` (a borrowed byte slice).
///
/// # Safety
/// `txn` must be valid; `table` must be a NUL-terminated UTF-8 C string;
/// `pk.data` if non-null must be valid for `pk.len` bytes.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_delete_by_pk(
    txn: mongreldb_transaction_t,
    table: *const c_char,
    pk: ByteSlice,
) -> i32 {
    clear();
    let Some(t) = as_txn(txn) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if table.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "table is null").as_return();
    }
    let table_name = cstr_to_string(table, "table name");
    if let Err(e) = t.db.table(&table_name) {
        return set_error(&e).as_return();
    }
    let pk_bytes = pk.to_vec();
    if let Ok(mut staging) = t.staging.lock() {
        staging.push(StagedOp::DeleteByPk {
            table: table_name,
            pk: pk_bytes,
        });
    }
    0
}

// ── commit ────────────────────────────────────────────────────────────────

/// Replay all staged ops into a fresh core transaction and commit atomically.
/// Returns 0 on success and writes the commit epoch into `out_epoch` (if
/// non-null). On conflict, returns a negative [`ErrorCode::Conflict`] and the
/// staged buffer is *not* cleared (the caller may adjust and retry, or
/// rollback).
///
/// # Safety
/// `txn` must be a valid transaction handle; `out_epoch` if non-null must be a
/// valid `u64` slot.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_txn_commit(
    txn: mongreldb_transaction_t,
    out_epoch: *mut u64,
) -> i32 {
    clear();
    let Some(t) = as_txn(txn) else {
        return ErrorCode::InvalidArgument.as_return();
    };

    // Take the staged ops out of the buffer. On commit failure we'll put them
    // back so the caller can retry.
    let stage = match t.staging.lock() {
        Ok(mut g) => std::mem::take(&mut *g),
        Err(_) => {
            return set_error_msg(ErrorCode::Unknown, "transaction buffer poisoned").as_return()
        }
    };

    match apply_txn(&t.db, &stage) {
        Ok(epoch) => {
            if !out_epoch.is_null() {
                *out_epoch = epoch.0;
            }
            0
        }
        Err(code) => {
            // Put the ops back so the caller can retry after fixing the issue.
            if let Ok(mut g) = t.staging.lock() {
                *g = stage;
            }
            code.as_return()
        }
    }
}

/// Replay the staged buffer into a fresh core transaction.
fn apply_txn(db: &CoreDatabase, stage: &[StagedOp]) -> Result<Epoch, ErrorCode> {
    db.transaction_for_current_principal_with_epoch(|tx| {
        for op in stage {
            match op {
                StagedOp::Put { table, cells } => {
                    tx.put_returning(table, cells.clone())?;
                }
                StagedOp::Upsert {
                    table,
                    cells,
                    update_cells,
                } => {
                    let action = if update_cells.is_empty() {
                        UpsertAction::DoNothing
                    } else {
                        UpsertAction::DoUpdate(update_cells.clone())
                    };
                    tx.upsert(table, cells.clone(), action)?;
                }
                StagedOp::Delete { table, row_id } => {
                    tx.delete(table, *row_id)?;
                }
                StagedOp::DeleteByPk { table, pk } => {
                    let rows =
                        db.query_for_current_principal(table, &Query::pk(pk.clone()), None)?;
                    if let Some(row) = rows.first() {
                        tx.delete(table, row.row_id)?;
                    }
                }
            }
        }
        Ok(())
    })
    .map(|(epoch, ())| epoch)
    .map_err(|error| set_error(&error))
}

// Keep the `c_to_value` import reachable for symmetry with the table module
// (the cell-pair conversion goes through `cell_inputs_to_pairs`, but the
// import documents the value-marshal path).
#[allow(dead_code)]
fn _ensure_c_to_value_imported(
    v: &crate::value::CValue,
    ty: &TypeId,
) -> Result<Value, crate::error::ErrorCode> {
    // SAFETY: only invoked from this module's own tests with valid values.
    unsafe { c_to_value(v, ty) }
}

// Quiet unused-import warnings for symbols referenced only via helpers above.
#[allow(dead_code)]
fn _ensure_query_imported(_q: &Query) {}
