//! Kit query builder execution FFI. Each function takes a JSON-encoded Kit
//! `Query` AST variant, runs it in a short-lived transaction, and returns the
//! results as a JSON string.
//!
//! For read queries (select, join, aggregate), the result is a JSON array of
//! row objects. For write queries (insert, update, upsert, delete), the result
//! is a JSON array of the returning values (or `[]` if no returning clause).
//!
//! The transaction lifetime problem (Kit's `Transaction<'_>` borrows the
//! Database) is handled by creating the transaction inside the FFI function,
//! running the query, and committing before returning. This matches how the
//! Python binding's closure-based `transaction()` helper works.

use crate::database::{as_kit_db, mongreldb_kit_database_t};
use crate::error::{clear, parse_cstr, set_error, set_error_msg, write_json_out, KitErrorCode};
use mongreldb_kit_core::{
    AggregateQuery, Delete, Insert, JoinQuery, Query, Select, Update, Upsert,
};
use std::os::raw::c_char;

/// Serialize a Vec of Kit Row values (the `values` map of each row) as a JSON
/// array, matching the Python binding's `row_to_json` pattern (row_id is
/// dropped).
fn rows_to_json(rows: &[kit::Row]) -> Result<String, serde_json::Error> {
    let maps: Vec<_> = rows.iter().map(|r| &r.values).collect();
    serde_json::to_string(&maps)
}

/// Serialize JoinRow results. JoinRow is `Map<String, Value>` (the merged
/// columns from both sides of the join), so we serialize the Vec of maps.
fn join_rows_to_json(rows: &[kit::JoinRow]) -> Result<String, serde_json::Error> {
    serde_json::to_string(rows)
}

/// Serialize returning values (Vec<serde_json::Value>).
fn values_to_json(values: &[serde_json::Value]) -> Result<String, serde_json::Error> {
    serde_json::to_string(values)
}

// ── read queries ───────────────────────────────────────────────────────────

/// Run a SELECT query. Takes a JSON-encoded `Select` AST, returns a JSON array
/// of row objects via `*out_json`.
///
/// # Safety
/// `db` must be valid; `query_json` must be a NUL-terminated C string;
/// `out_json` must be a valid non-null pointer.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_select_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let select: Select = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse Select query JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h.db.borrow().transaction(0, |txn| txn.select(&Query::Select(select.clone())));
    let rows = match result {
        Ok(r) => r,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(rows_to_json(&rows), out_json)
}

/// Run a JOIN query. Takes a JSON-encoded `JoinQuery` AST, returns a JSON array
/// of `{left, right}` objects via `*out_json`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_join_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let jq: JoinQuery = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse JoinQuery JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h.db.borrow().transaction(0, |txn| txn.join(&jq.clone()));
    let rows = match result {
        Ok(r) => r,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(join_rows_to_json(&rows), out_json)
}

/// Run an AGGREGATE query. Takes a JSON-encoded `AggregateQuery` AST, returns
/// a JSON array of result rows via `*out_json`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_aggregate_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let agg: AggregateQuery = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse AggregateQuery JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h.db.borrow().transaction(0, |txn| txn.aggregate(&agg.clone()));
    let rows = match result {
        Ok(r) => r,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(rows_to_json(&rows), out_json)
}

// ── write queries ──────────────────────────────────────────────────────────

/// Run an INSERT query. Takes a JSON-encoded `Insert` AST, returns a JSON
/// array of returning values via `*out_json`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_insert_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let ins: Insert = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse Insert query JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h
        .db
        .borrow()
        .transaction(0, |txn| txn.execute(&Query::Insert(ins.clone())));
    let vals = match result {
        Ok(v) => v,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(values_to_json(&vals), out_json)
}

/// Run an UPDATE query. Takes a JSON-encoded `Update` AST, returns a JSON
/// array of returning values via `*out_json`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_update_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let upd: Update = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse Update query JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h
        .db
        .borrow()
        .transaction(0, |txn| txn.execute(&Query::Update(upd.clone())));
    let vals = match result {
        Ok(v) => v,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(values_to_json(&vals), out_json)
}

/// Run an UPSERT query. Takes a JSON-encoded `Upsert` AST, returns a JSON
/// array of returning values via `*out_json`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_upsert_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let ups: Upsert = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse Upsert query JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h
        .db
        .borrow()
        .transaction(0, |txn| txn.execute(&Query::Upsert(ups.clone())));
    let vals = match result {
        Ok(v) => v,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(values_to_json(&vals), out_json)
}

/// Run a DELETE query. Takes a JSON-encoded `Delete` AST, returns a JSON
/// array of returning values via `*out_json`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_query_delete_json(
    db: mongreldb_kit_database_t,
    query_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let qstr = match parse_cstr(query_json, "query_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }
    let del: Delete = match serde_json::from_str(qstr) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse Delete query JSON: {e}"),
            )
            .as_return();
        }
    };

    let result = h
        .db
        .borrow()
        .transaction(0, |txn| txn.execute(&Query::Delete(del.clone())));
    let vals = match result {
        Ok(v) => v,
        Err(e) => return set_error(&e).as_return(),
    };
    write_json_out(values_to_json(&vals), out_json)
}
