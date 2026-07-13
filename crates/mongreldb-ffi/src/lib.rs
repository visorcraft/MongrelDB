//! MongrelDB C ABI (`mongreldb-ffi`) — a stable, language-agnostic C interface
//! over [`mongreldb_core`], the foundation for native language bindings.
//!
//! This crate exposes a **C ABI** (not SQL): opaque handles for databases,
//! tables, transactions, schemas, and queries; `int32_t` return codes (0 = OK,
//! negative = error code); NUL-terminated UTF-8 strings in, FFI-owned strings
//! out; and a thread-local last-error store for human-readable messages.
//!
//! # Design rules
//!
//! - Every `#[no_mangle] extern "C"` function returns `int32_t` (0=OK,
//!   negative=error) unless it returns an opaque pointer handle (`*mut` or
//!   null on error).
//! - Opaque handle types: `mongreldb_database_t`, `mongreldb_transaction_t`,
//!   `mongreldb_schema_builder_t`, `mongreldb_schema_t`, `mongreldb_query_t`,
//!   `mongreldb_result_t`, `mongreldb_table_t`.
//! - C strings in: `*const c_char` (NUL-terminated UTF-8). C strings out:
//!   `*const c_char` owned by the FFI layer (valid until the next call or a
//!   free function).
//! - Byte slices: `{ const uint8_t *data; size_t len; }`.
//! - Memory: handles use `Box::into_raw` / `Box::from_raw`. Out-strings use
//!   `CString::into_raw` / matching free functions.
//! - Transactions use the **staging-buffer pattern**: ops are buffered and
//!   replayed into a core `Transaction` at commit (the engine's
//!   `Transaction<'db>` lifetime cannot cross the FFI boundary).
//! - Table handles **re-resolve** via `db.table(&name)` on each call (safe
//!   against drop/rename).
//! - `#[repr(C)]` on all public C structs and enums; `#[no_mangle] pub extern
//!   "C"` on all exported functions. Error messages set via the thread-local
//!   `set_error()` from [`error`].
//!
//! # Modules
//!
//! - [`error`] — error codes + thread-local last-error capture.
//! - [`value`] — `#[repr(C)]` tagged union mirror of `core::Value`.
//! - [`schema`] — `#[repr(C)]` schema enums/structs + builder.
//! - [`database`] — `FFIDatabase` + lifecycle FFI functions.
//! - [`query`] — `#[repr(C)]` condition union + `FFIQuery` builder.
//! - [`table`] — `FFITable`, `FFIResult`, row/cell structs + table FFI.
//! - [`transaction`] — `FFITransaction` staging-buffer + FFI.
//! - [`auth`] — user/role/credential FFI wrappers.
//! - [`cstr`] — shared C-string / handle plumbing helpers.

#![allow(clippy::missing_safety_doc)]

pub mod auth;
pub mod cstr;
pub mod database;
pub mod error;
pub mod migrate;
pub mod query;
pub mod schema;
pub mod sql;
pub mod table;
pub mod transaction;
pub mod value;

// ── re-exports of all `#[no_mangle] extern "C"` symbols ───────────────────
//
// Re-exporting every public function and type keeps a single discoverable
// surface (and lets a future header generator walk `mongreldb_ffi::*`).
//
// Error accessors.
pub use error::{mongreldb_free_error_string, mongreldb_last_error, mongreldb_last_error_code};

// String free.
pub use database::mongreldb_free_string;

// Database lifecycle.
pub use database::{
    mongreldb_create, mongreldb_create_table, mongreldb_create_with_credentials,
    mongreldb_database_close, mongreldb_database_compact, mongreldb_database_compact_table,
    mongreldb_database_free, mongreldb_database_table_names, mongreldb_drop_table, mongreldb_open,
    mongreldb_open_with_credentials, mongreldb_rename_table,
};

// SQL execution (DataFusion via MongrelSession; returns Arrow IPC file bytes).
pub use sql::{mongreldb_database_sql, mongreldb_database_sql_refresh, mongreldb_free_sql_result};

// Migration planning and checksums (JSON in/out, language-neutral).
pub use migrate::{
    mongreldb_free_migrate_string, mongreldb_migration_checksum_json,
    mongreldb_plan_migrations_json,
};

// `mongreldb_database_table` lives in the `table` module (it returns a table
// handle); re-export it alongside the other table FFI functions below.

// Schema builder.
pub use schema::{
    mongreldb_schema_add_column, mongreldb_schema_add_foreign_key, mongreldb_schema_add_index,
    mongreldb_schema_add_unique, mongreldb_schema_begin, mongreldb_schema_build,
    mongreldb_schema_builder_free, mongreldb_schema_free, mongreldb_schema_set_clustered,
};

// Query builder.
pub use query::{
    mongreldb_minhash_member_hash_v1_json, mongreldb_query_add, mongreldb_query_begin,
    mongreldb_query_build, mongreldb_query_free, mongreldb_query_set_limit,
    mongreldb_query_set_projection,
};

// Table ops + result iteration.
pub use table::{
    mongreldb_ann_rerank_result_count, mongreldb_ann_rerank_result_free,
    mongreldb_ann_rerank_result_hit, mongreldb_database_table, mongreldb_result_count,
    mongreldb_result_free, mongreldb_result_row, mongreldb_row_cell, mongreldb_row_cell_count,
    mongreldb_table_ann_rerank, mongreldb_table_count, mongreldb_table_delete,
    mongreldb_table_free, mongreldb_table_put, mongreldb_table_put_batch, mongreldb_table_query,
};

// Transaction (staging buffer).
pub use transaction::{
    mongreldb_begin, mongreldb_txn_commit, mongreldb_txn_delete, mongreldb_txn_delete_by_pk,
    mongreldb_txn_free, mongreldb_txn_put, mongreldb_txn_rollback, mongreldb_txn_upsert,
};

// Auth / users / roles / permissions.
pub use auth::{
    mongreldb_alter_user_password, mongreldb_create_role, mongreldb_create_user,
    mongreldb_disable_auth, mongreldb_drop_role, mongreldb_drop_user, mongreldb_enable_auth,
    mongreldb_grant_permission, mongreldb_grant_role, mongreldb_revoke_permission,
    mongreldb_revoke_role, mongreldb_set_user_admin, mongreldb_verify_user,
};

// Public C-facing types (so bindings can reference them by full path).
pub use database::mongreldb_database_t;
pub use query::mongreldb_query_t;
pub use schema::{
    mongreldb_column_def, mongreldb_fk_action, mongreldb_foreign_key, mongreldb_index_def,
    mongreldb_index_kind, mongreldb_schema_builder_t, mongreldb_schema_t, mongreldb_type_id,
    mongreldb_unique_constraint, StringArray, U16Slice, MONGRELDB_COL_AUTO_INCREMENT,
    MONGRELDB_COL_EMBEDDING_BINARY_QUANTIZED, MONGRELDB_COL_ENCRYPTED,
    MONGRELDB_COL_ENCRYPTED_INDEXABLE, MONGRELDB_COL_NULLABLE, MONGRELDB_COL_PRIMARY_KEY,
};
pub use table::{
    mongreldb_ann_rerank_hit, mongreldb_ann_rerank_result_t, mongreldb_cell, mongreldb_cell_input,
    mongreldb_cell_input_array, mongreldb_cell_slice, mongreldb_result_t, mongreldb_row,
    mongreldb_row_input_array, mongreldb_table_t, mongreldb_vector_metric,
};
pub use transaction::mongreldb_transaction_t;
pub use value::{
    ByteSlice, CDecimal128, CInterval, CValue, CValuePayload, CValueTag, EmbeddingSlice,
};
