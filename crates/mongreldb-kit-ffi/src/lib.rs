//! MongrelDB Kit C ABI (`mongreldb-kit-ffi`) — a stable C interface over
//! `mongreldb-kit`, providing the full Kit surface (schema model, migrations,
//! query builder execution, and SQL) for native language bindings.
//!
//! This crate complements `mongreldb-ffi` (the core engine ABI): link both
//! `libmongreldb` and `libmongreldb_kit` to get the complete Tier-1 experience
//! (raw columnar engine + Kit's schema-aware layer).
//!
//! # Surface
//!
//! - **Kit Database** lifecycle: open/create (with JSON schema), close/free.
//! - **SQL**: `mongreldb_kit_sql_rows` (JSON results) and
//!   `mongreldb_kit_sql_arrow` (Arrow IPC bytes).
//! - **Migrations**: `mongreldb_kit_migrate` (full Kit runner, JSON in).
//! - **Query builder execution**: select/join/aggregate/upsert/insert/update/
//!   delete via JSON-encoded Kit `Query` AST.
//! - **Validation**: `mongreldb_kit_validate_row` (JSON table schema + row in).
//!
//! # Design
//!
//! All complex types (Schema, Query AST, Migration, Row) cross the boundary as
//! JSON strings, matching the pattern established by the PyO3 binding. The
//! host language serializes/deserializes the JSON; the FFI does the execution.
//!
//! Results are returned as JSON strings (for query results) or Arrow IPC bytes
//! (for SQL with Arrow format), owned by the caller and freed with the
//! matching free functions.

#![allow(clippy::missing_safety_doc)]

pub mod build_info;
pub mod database;
pub mod error;
pub mod migrate;
pub mod query;
pub mod sql;

// ── re-exports ────────────────────────────────────────────────────────────
pub use build_info::mongreldb_kit_build_info;
pub use database::{
    mongreldb_kit_create, mongreldb_kit_create_encrypted, mongreldb_kit_create_with_credentials,
    mongreldb_kit_database_free, mongreldb_kit_open, mongreldb_kit_open_encrypted,
    mongreldb_kit_open_with_credentials, mongreldb_kit_refresh_sql_session,
};
pub use error::{
    mongreldb_kit_free_error_string, mongreldb_kit_free_json, mongreldb_kit_last_error,
    mongreldb_kit_last_error_code,
};
pub use migrate::{mongreldb_kit_applied_migrations_json, mongreldb_kit_migrate_json};
pub use query::{
    mongreldb_kit_query_aggregate_json, mongreldb_kit_query_delete_json,
    mongreldb_kit_query_insert_json, mongreldb_kit_query_join_json,
    mongreldb_kit_query_select_json, mongreldb_kit_query_update_json,
    mongreldb_kit_query_upsert_json,
};
pub use sql::{mongreldb_kit_free_arrow, mongreldb_kit_sql_arrow, mongreldb_kit_sql_rows};

// Handle type.
pub use database::mongreldb_kit_database_t;
