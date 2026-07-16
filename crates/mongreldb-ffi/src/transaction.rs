//! `FFITransaction` — the staging-buffer transaction pattern (matching the
//! NAPI addon). A transaction holds `Arc<Database>` plus one mutex-protected
//! state value.
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
use crate::error::{
    clear, copy_c_text, mongreldb_error_details_v1, set_error, set_error_msg,
    set_error_with_details, ErrorCode,
};
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
    state: Mutex<FFITransactionState>,
}

#[derive(Default)]
struct FFITransactionState {
    staging: Vec<StagedOp>,
    committed_epoch: Option<u64>,
    rolled_back: bool,
    terminal_error: Option<TerminalCommitError>,
}

struct TerminalCommitError {
    code: ErrorCode,
    epoch: u64,
    outcome_known: bool,
    message: String,
}

impl FFITransaction {
    pub fn new(db: Arc<CoreDatabase>) -> Self {
        Self {
            db,
            state: Mutex::new(FFITransactionState::default()),
        }
    }

    pub fn into_handle(self) -> mongreldb_transaction_t {
        Box::into_raw(Box::new(self)) as mongreldb_transaction_t
    }
}

fn stage_op(transaction: &FFITransaction, operation: StagedOp) -> i32 {
    let mut state = match transaction.state.lock() {
        Ok(state) => state,
        Err(_) => {
            return set_error_msg(ErrorCode::Unknown, "transaction buffer poisoned").as_return()
        }
    };
    if let Some(epoch) = state.committed_epoch {
        return set_committed_transaction_error(epoch);
    }
    if state.rolled_back {
        return set_rolled_back_transaction_error();
    }
    if let Some(error) = state.terminal_error.as_ref() {
        return set_terminal_commit_error(error);
    }
    state.staging.push(operation);
    0
}

fn ensure_active(transaction: &FFITransaction) -> Result<(), i32> {
    let state = transaction.state.lock().map_err(|_| {
        set_error_msg(ErrorCode::Unknown, "transaction buffer poisoned").as_return()
    })?;
    if let Some(epoch) = state.committed_epoch {
        return Err(set_committed_transaction_error(epoch));
    }
    if state.rolled_back {
        return Err(set_rolled_back_transaction_error());
    }
    match state.terminal_error.as_ref() {
        Some(error) => Err(set_terminal_commit_error(error)),
        None => Ok(()),
    }
}

fn set_committed_transaction_error(epoch: u64) -> i32 {
    let message = format!("transaction already committed at epoch {epoch}");
    let mut details = mongreldb_error_details_v1 {
        code: ErrorCode::InvalidArgument.as_return(),
        outcome_known: 1,
        committed: 1,
        has_last_commit_epoch: 1,
        last_commit_epoch: epoch,
        retryable: 0,
        ..Default::default()
    };
    copy_c_text(&mut details.server_state, "completed");
    set_error_with_details(ErrorCode::InvalidArgument, &message, details).as_return()
}

fn set_rolled_back_transaction_error() -> i32 {
    let mut details = mongreldb_error_details_v1 {
        code: ErrorCode::InvalidArgument.as_return(),
        outcome_known: 1,
        committed: 0,
        retryable: 0,
        ..Default::default()
    };
    copy_c_text(&mut details.server_state, "rolled_back");
    set_error_with_details(
        ErrorCode::InvalidArgument,
        "transaction already rolled back",
        details,
    )
    .as_return()
}

fn set_terminal_commit_error(error: &TerminalCommitError) -> i32 {
    let mut details = mongreldb_error_details_v1 {
        code: error.code.as_return(),
        outcome_known: u8::from(error.outcome_known),
        committed: u8::from(error.outcome_known),
        has_last_commit_epoch: 1,
        last_commit_epoch: error.epoch,
        retryable: 0,
        ..Default::default()
    };
    copy_c_text(&mut details.server_state, "failed");
    set_error_with_details(error.code, &error.message, details).as_return()
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
    let mut state = match t.state.lock() {
        Ok(state) => state,
        Err(_) => {
            return set_error_msg(ErrorCode::Unknown, "transaction buffer poisoned").as_return()
        }
    };
    if let Some(epoch) = state.committed_epoch {
        return set_committed_transaction_error(epoch);
    }
    if state.rolled_back {
        return 0;
    }
    if let Some(error) = state.terminal_error.as_ref() {
        return set_terminal_commit_error(error);
    }
    state.staging.clear();
    state.rolled_back = true;
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
    if let Err(code) = ensure_active(t) {
        return code;
    }
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
    stage_op(
        t,
        StagedOp::Put {
            table: table_name,
            cells: cols,
        },
    )
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
    if let Err(code) = ensure_active(t) {
        return code;
    }
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
    stage_op(
        t,
        StagedOp::Upsert {
            table: table_name,
            cells: insert_cols,
            update_cells: update_cols,
        },
    )
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
    if let Err(code) = ensure_active(t) {
        return code;
    }
    if table.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "table is null").as_return();
    }
    let table_name = cstr_to_string(table, "table name");
    // Validate the table exists.
    if let Err(e) = t.db.table(&table_name) {
        return set_error(&e).as_return();
    }
    stage_op(
        t,
        StagedOp::Delete {
            table: table_name,
            row_id: RowId(row_id),
        },
    )
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
    if let Err(code) = ensure_active(t) {
        return code;
    }
    if table.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "table is null").as_return();
    }
    let table_name = cstr_to_string(table, "table name");
    if let Err(e) = t.db.table(&table_name) {
        return set_error(&e).as_return();
    }
    let pk_bytes = pk.to_vec();
    stage_op(
        t,
        StagedOp::DeleteByPk {
            table: table_name,
            pk: pk_bytes,
        },
    )
}

// ── commit ────────────────────────────────────────────────────────────────

/// Replay all staged ops into a fresh core transaction and commit atomically.
/// Returns 0 on success and writes the commit epoch into `out_epoch` (if
/// non-null). Repeating commit on the same successfully committed handle is
/// idempotent and returns the same epoch. On conflict, returns a negative
/// [`ErrorCode::Conflict`] and the staged buffer is *not* cleared (the caller
/// may adjust and retry, or rollback).
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

    // Take the staged ops out of the buffer. Only a known pre-commit failure
    // may restore them. A durable or unknown outcome must never be retryable.
    let mut state = match t.state.lock() {
        Ok(state) => state,
        Err(_) => {
            return set_error_msg(ErrorCode::Unknown, "transaction buffer poisoned").as_return()
        }
    };
    if let Some(error) = state.terminal_error.as_ref() {
        if !out_epoch.is_null() {
            *out_epoch = error.epoch;
        }
        return set_terminal_commit_error(error);
    }
    if let Some(epoch) = state.committed_epoch {
        if !out_epoch.is_null() {
            *out_epoch = epoch;
        }
        return 0;
    }
    if state.rolled_back {
        return set_rolled_back_transaction_error();
    }
    let stage = std::mem::take(&mut state.staging);

    match apply_txn(&t.db, &stage) {
        Ok(epoch) => {
            state.committed_epoch = Some(epoch.0);
            if !out_epoch.is_null() {
                *out_epoch = epoch.0;
            }
            0
        }
        Err(error) => {
            let terminal = match &error {
                mongreldb_core::MongrelError::DurableCommit { epoch, .. } => {
                    Some((ErrorCode::CommitOutcome, *epoch))
                }
                mongreldb_core::MongrelError::CommitOutcomeUnknown { epoch, .. } => {
                    Some((ErrorCode::OutcomeUnknown, *epoch))
                }
                _ => None,
            };
            if let Some((code, epoch)) = terminal {
                if !out_epoch.is_null() {
                    *out_epoch = epoch;
                }
                state.terminal_error = Some(TerminalCommitError {
                    code,
                    epoch,
                    outcome_known: code == ErrorCode::CommitOutcome,
                    message: error.to_string(),
                });
            } else {
                // A known pre-commit failure is safe to correct and retry.
                state.staging = stage;
            }
            set_error(&error).as_return()
        }
    }
}

/// Replay the staged buffer into a fresh core transaction.
fn apply_txn(db: &CoreDatabase, stage: &[StagedOp]) -> mongreldb_core::Result<Epoch> {
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
