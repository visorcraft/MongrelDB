/*
 * mongreldb_kit.h - C ABI for MongrelDB Kit.
 *
 * This header is the interface for the Kit layer (schema model, migrations,
 * query builder execution, and SQL). It complements mongreldb.h (the core
 * engine ABI): link both libmongreldb and libmongreldb_kit to get the full
 * Tier-1 experience.
 *
 * Memory model:
 *   - Database handles are owned by the caller. Free with
 *     mongreldb_kit_database_free().
 *   - Out-strings (JSON results) are owned by the caller. Free with
 *     mongreldb_kit_free_json().
 *   - Arrow IPC byte buffers are owned by the caller. Free with
 *     mongreldb_kit_free_arrow().
 *
 * Thread safety:
 *   - Database handles use Rc<RefCell> internally (single-threaded). Do NOT
 *     share across threads. If you need multi-threaded access, create one
 *     handle per thread.
 *
 * All complex types (Schema, Query AST, Migration, Row) cross the boundary as
 * JSON strings. Results are returned as JSON strings or Arrow IPC bytes.
 */

#ifndef MONGRELDB_KIT_H
#define MONGRELDB_KIT_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handle ──────────────────────────────────────────────────────── */

typedef struct mongreldb_kit_database mongreldb_kit_database_t;

/* ── Error accessors ────────────────────────────────────────────────────── */

/* Last error message (valid until the next FFI call on this thread). */
const char *mongreldb_kit_last_error(void);
/* Last error code (0 = no error, negative = error). */
int32_t mongreldb_kit_last_error_code(void);
/* Free an error string. */
void mongreldb_kit_free_error_string(char *ptr);

/* ── Database lifecycle ─────────────────────────────────────────────────── */

/* Open an existing Kit database. Returns NULL on error. */
mongreldb_kit_database_t *mongreldb_kit_open(const char *path);

/* Create a fresh Kit database with a JSON schema. Returns NULL on error. */
mongreldb_kit_database_t *mongreldb_kit_create(
    const char *path, const char *schema_json);

/* Open an encrypted Kit database. Returns NULL on error. */
mongreldb_kit_database_t *mongreldb_kit_open_encrypted(
    const char *path, const char *passphrase);

/* Create an encrypted Kit database. Returns NULL on error. */
mongreldb_kit_database_t *mongreldb_kit_create_encrypted(
    const char *path, const char *schema_json, const char *passphrase);

/* Open with authentication. Returns NULL on error. */
mongreldb_kit_database_t *mongreldb_kit_open_with_credentials(
    const char *path, const char *user, const char *password);

/* Create with authentication + admin user. Returns NULL on error. */
mongreldb_kit_database_t *mongreldb_kit_create_with_credentials(
    const char *path, const char *schema_json,
    const char *admin_user, const char *admin_password);

/* Rebuild the SQL session after schema changes. Returns MDB_OK (0) on success. */
int32_t mongreldb_kit_refresh_sql_session(mongreldb_kit_database_t *db);

/* Free a database handle. */
void mongreldb_kit_database_free(mongreldb_kit_database_t *db);

/* ── SQL execution ──────────────────────────────────────────────────────── */
/*
 * Run SQL via the Kit Database's MongrelSession (DataFusion). Results are
 * returned as either a JSON array of row objects (sql_rows) or Arrow IPC
 * file bytes (sql_arrow). DDL/DML returns [] or a zero-length buffer.
 */

/* SQL → JSON array of row objects. Caller frees *out_json. */
int32_t mongreldb_kit_sql_rows(
    mongreldb_kit_database_t *db, const char *sql,
    const char **out_json);

/* SQL → Arrow IPC file bytes. Caller frees *out_buf. */
int32_t mongreldb_kit_sql_arrow(
    mongreldb_kit_database_t *db, const char *sql,
    uint8_t **out_buf, size_t *out_len);

/* Free an Arrow IPC buffer. */
void mongreldb_kit_free_arrow(uint8_t *ptr, size_t len);

/* Free a JSON result string. */
void mongreldb_kit_free_json(char *ptr);

/* ── Migrations ─────────────────────────────────────────────────────────── */
/*
 * The full Kit migration runner. Applies each MigrationOp to the database.
 * Already-applied migrations (version <= highest recorded) are skipped.
 */

/* Run migrations from a JSON array. Returns MDB_OK (0) on success. */
int32_t mongreldb_kit_migrate_json(
    mongreldb_kit_database_t *db, const char *migrations_json);

/* Read applied migrations as a JSON array. Caller frees *out_json. */
int32_t mongreldb_kit_applied_migrations_json(
    mongreldb_kit_database_t *db, const char **out_json);

/* ── Query builder execution ────────────────────────────────────────────── */
/*
 * Each function takes a JSON-encoded Kit Query AST variant and returns a JSON
 * array of results. Read queries return row objects; write queries return the
 * returning values (or [] if no returning clause).
 */

/* SELECT → JSON array of row objects. */
int32_t mongreldb_kit_query_select_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

/* JOIN → JSON array of merged row objects. */
int32_t mongreldb_kit_query_join_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

/* AGGREGATE → JSON array of aggregate result rows. */
int32_t mongreldb_kit_query_aggregate_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

/* INSERT → JSON array of returning values. */
int32_t mongreldb_kit_query_insert_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

/* UPDATE → JSON array of returning values. */
int32_t mongreldb_kit_query_update_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

/* UPSERT → JSON array of returning values. */
int32_t mongreldb_kit_query_upsert_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

/* DELETE → JSON array of returning values. */
int32_t mongreldb_kit_query_delete_json(
    mongreldb_kit_database_t *db, const char *query_json,
    const char **out_json);

#ifdef __cplusplus
}
#endif

#endif /* MONGRELDB_KIT_H */
/** Runtime build identity JSON. Free with mongreldb_kit_free_json(). */
char *mongreldb_kit_build_info(void);
