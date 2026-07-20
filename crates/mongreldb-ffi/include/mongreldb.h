/*
 * mongreldb.h — C ABI for MongrelDB core.
 *
 * This header is the single interface for all native (Tier-1) language
 * bindings (Go, Java, C#, Ruby, Swift, etc.). Every function returns
 * int32_t (0 = MDB_OK, negative = error code) unless it returns an
 * opaque handle pointer (NULL on error). Detailed error messages are
 * available via mongreldb_last_error() / mongreldb_last_error_code().
 *
 * Memory model:
 *   - Handles (database, table, transaction, etc.) are owned by the caller.
 *     Free them with the matching *_free function.
 *   - Out-strings from mongreldb_last_error() are valid until the next call
 *     on the same thread; free persistent strings with mongreldb_free_string().
 *   - Result handles from mongreldb_table_query() must be freed with
 *     mongreldb_result_free().
 *
 * Thread safety:
 *   - Database handles are Send + Sync (safe to share across threads).
 *   - Table and transaction handles are NOT thread-safe (one thread at a time).
 *   - mongreldb_last_error() is thread-local.
 */

#ifndef MONGRELDB_H
#define MONGRELDB_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handle types ────────────────────────────────────────────────── */

typedef struct mongreldb_database       mongreldb_database_t;
typedef struct mongreldb_sql_query      mongreldb_sql_query_t;
typedef struct mongreldb_table          mongreldb_table_t;
typedef struct mongreldb_transaction    mongreldb_transaction_t;
typedef struct mongreldb_schema_builder mongreldb_schema_builder_t;
typedef struct mongreldb_schema         mongreldb_schema_t;
typedef struct mongreldb_query          mongreldb_query_t;
typedef struct mongreldb_result         mongreldb_result_t;
typedef struct mongreldb_ann_rerank_result mongreldb_ann_rerank_result_t;
typedef struct mongreldb_search_request_builder mongreldb_search_request_t;

/* ── Error codes ────────────────────────────────────────────────────────── */

#define MDB_OK                    0
#define MDB_ERR_INVALID_ARGUMENT -1
#define MDB_ERR_INVALID_ARG      MDB_ERR_INVALID_ARGUMENT
#define MDB_ERR_NOT_FOUND        -2
#define MDB_ERR_CONFLICT         -3
#define MDB_ERR_SCHEMA           -4
#define MDB_ERR_COLUMN_NOT_FOUND -5
#define MDB_ERR_UNAUTHORIZED     -6
#define MDB_ERR_FULL             -7
#define MDB_ERR_IO               -8
#define MDB_ERR_QUERY_CANCELLED  -9
#define MDB_ERR_DEADLINE_EXCEEDED -10
#define MDB_ERR_QUERY_ID_CONFLICT -11
#define MDB_ERR_QUERY_REGISTRY_FULL -12
#define MDB_ERR_TRANSACTION_STATE -13
#define MDB_ERR_INVALID_QUERY_STATE -14
#define MDB_ERR_COMMIT_OUTCOME   -15
#define MDB_ERR_OUTCOME_UNKNOWN  -16
#define MDB_ERR_RESULT_LIMIT     -17
#define MDB_ERR_SERIALIZATION    -18
#define MDB_ERR_SQL_EXECUTION    -19
#define MDB_ERR_QUERY_CANCELLED_AFTER_COMMIT -20
#define MDB_ERR_DEADLINE_AFTER_COMMIT -21
#define MDB_ERR_SERIALIZATION_AFTER_COMMIT -22
#define MDB_ERR_TRANSACTION_ABORTED -23
/* Reserved for remote clients layered on this ABI. Embedded SQL does not
 * negotiate daemon capabilities. */
#define MDB_ERR_CAPABILITY_UNSUPPORTED -24
#define MDB_ERR_UNKNOWN          -99

/* ── Column flags (bitmask) ─────────────────────────────────────────────── */

#define MDB_COL_NULLABLE                   1u
#define MDB_COL_PRIMARY_KEY                2u
#define MDB_COL_ENCRYPTED                  4u
#define MDB_COL_ENCRYPTED_INDEXABLE        8u
#define MDB_COL_EMBEDDING_BINARY_QUANTIZED 16u
#define MDB_COL_AUTO_INCREMENT             32u

/* ── Type IDs ───────────────────────────────────────────────────────────── */

typedef enum {
    MDB_TYPE_BOOL       = 0,
    MDB_TYPE_INT8       = 1,
    MDB_TYPE_INT16      = 2,
    MDB_TYPE_INT32      = 3,
    MDB_TYPE_INT64      = 4,
    MDB_TYPE_UINT8      = 5,
    MDB_TYPE_UINT16     = 6,
    MDB_TYPE_UINT32     = 7,
    MDB_TYPE_UINT64     = 8,
    MDB_TYPE_FLOAT32    = 9,
    MDB_TYPE_FLOAT64    = 10,
    MDB_TYPE_TIMESTAMP  = 11,
    MDB_TYPE_DATE32     = 12,
    MDB_TYPE_DATE64     = 13,
    MDB_TYPE_TIME64     = 14,
    MDB_TYPE_INTERVAL   = 15,
    MDB_TYPE_UUID       = 16,
    MDB_TYPE_JSON       = 17,
    MDB_TYPE_ARRAY      = 18,
    MDB_TYPE_BYTES      = 19,
    MDB_TYPE_EMBEDDING  = 20,
    MDB_TYPE_DECIMAL128 = 21,
    MDB_TYPE_ENUM       = 22,
} mongreldb_type_id;

/* ── Index kind ─────────────────────────────────────────────────────────── */

typedef enum {
    MDB_INDEX_BITMAP       = 0,
    MDB_INDEX_FM           = 1,
    MDB_INDEX_ANN          = 2,
    MDB_INDEX_LEARNED_RANGE = 3,
    MDB_INDEX_MIN_HASH     = 4,
    MDB_INDEX_SPARSE       = 5,
} mongreldb_index_kind;

/* ── FK action ──────────────────────────────────────────────────────────── */

typedef enum {
    MDB_FK_RESTRICT = 0,
    MDB_FK_CASCADE  = 1,
    MDB_FK_SET_NULL = 2,
} mongreldb_fk_action;

/* ── Condition kind ─────────────────────────────────────────────────────── */

typedef enum {
    MDB_COND_PK             = 0,
    MDB_COND_BITMAP_EQ      = 1,
    MDB_COND_BITMAP_IN      = 2,
    MDB_COND_BYTES_PREFIX   = 3,
    MDB_COND_ANN            = 4,
    MDB_COND_FM_CONTAINS    = 5,
    MDB_COND_FM_CONTAINS_ALL = 6,
    MDB_COND_RANGE_INT      = 7,
    MDB_COND_RANGE_F64      = 8,
    MDB_COND_SPARSE_MATCH   = 9,
    MDB_COND_MIN_HASH       = 10,
    MDB_COND_IS_NULL        = 11,
    MDB_COND_IS_NOT_NULL    = 12,
} mongreldb_condition_kind;

typedef enum {
    MDB_VECTOR_COSINE    = 0,
    MDB_VECTOR_DOT       = 1,
    MDB_VECTOR_EUCLIDEAN = 2,
} mongreldb_vector_metric;

/* ANN candidate distance kind for mongreldb_ann_rerank_hit. */
typedef enum {
    MDB_ANN_CANDIDATE_HAMMING = 0,
    MDB_ANN_CANDIDATE_COSINE  = 1,
} mongreldb_ann_candidate_distance_kind;

/* ── Value tagged union ─────────────────────────────────────────────────── */

typedef enum {
    MDB_VALUE_NULL      = 0,
    MDB_VALUE_BOOL      = 1,
    MDB_VALUE_INT64     = 2,
    MDB_VALUE_FLOAT64   = 3,
    MDB_VALUE_BYTES     = 4,
    MDB_VALUE_EMBEDDING = 5,
    MDB_VALUE_DECIMAL   = 6,
    MDB_VALUE_INTERVAL  = 7,
    MDB_VALUE_UUID      = 8,
    MDB_VALUE_JSON      = 9,
} mongreldb_value_tag;

typedef struct {
    const uint8_t *data;
    size_t len;
} mongreldb_bytes_view;

typedef struct {
    const float *data;
    size_t len;
} mongreldb_embedding_view;

typedef struct {
    const mongreldb_bytes_view *items;
    size_t len;
} mongreldb_bytes_view_array;

typedef struct {
    uint32_t token;
    float weight;
} mongreldb_sparse_term;

typedef struct {
    const mongreldb_sparse_term *items;
    size_t len;
} mongreldb_sparse_term_array;

typedef struct {
    const char *const *items;
    size_t len;
} mongreldb_minhash_members;

typedef struct {
    int64_t months;
    int32_t days;
    int64_t nanos;
} mongreldb_interval;

typedef struct {
    int32_t tag;
    union {
        uint8_t b;
        int64_t i64;
        double f64;
        mongreldb_bytes_view bytes;
        mongreldb_embedding_view embedding;
        uint8_t decimal[16];
        mongreldb_interval interval;
        uint8_t uuid[16];
    } v;
} mongreldb_value;

/* ── Schema structs ─────────────────────────────────────────────────────── */

typedef struct {
    const char *const *items;
    size_t len;
} mongreldb_string_array;

typedef struct {
    const uint16_t *data;
    size_t len;
} mongreldb_u16_slice;

typedef struct {
    uint16_t id;
    const char *name;
    int32_t ty;
    uint32_t flags;
    uint32_t embedding_dim;
    uint8_t decimal_precision;
    int8_t decimal_scale;
    mongreldb_string_array enum_variants;
} mongreldb_column_def;

typedef struct {
    const char *name;
    uint16_t column_id;
    int32_t kind;
} mongreldb_index_def;

typedef struct {
    uint16_t id;
    const char *name;
    mongreldb_u16_slice columns;
} mongreldb_unique_constraint;

typedef struct {
    uint16_t id;
    const char *name;
    mongreldb_u16_slice columns;
    const char *ref_table;
    mongreldb_u16_slice ref_columns;
    int32_t on_delete;
    int32_t on_update;
} mongreldb_foreign_key;

/* ── Condition struct ───────────────────────────────────────────────────── */

typedef struct {
    int32_t kind;
    uint16_t column_id;
    int64_t int64_lo;
    int64_t int64_hi;
    double float64_lo;
    double float64_hi;
    uint8_t lo_inclusive;
    uint8_t hi_inclusive;
    uint32_t k;
    mongreldb_bytes_view bytes; /* PK key, BitmapEq value, FmContains pattern */
    mongreldb_bytes_view_array byte_values; /* BitmapIn, FmContainsAll */
    mongreldb_embedding_view embedding; /* ANN query vector */
    mongreldb_sparse_term_array sparse; /* SparseMatch */
    mongreldb_minhash_members minhash_members; /* MinHashSimilar */
} mongreldb_condition;

/* ── Table row/cell structs ─────────────────────────────────────────────── */

typedef struct {
    uint16_t column_id;
    mongreldb_value value;
} mongreldb_cell;

typedef struct {
    const mongreldb_cell *data;
    size_t len;
} mongreldb_cell_slice;

typedef struct {
    uint64_t row_id;
    mongreldb_cell_slice cells;
} mongreldb_row;

/*
 * Exact ANN rerank hit. candidate_distance_kind selects the valid payload:
 *   MDB_ANN_CANDIDATE_HAMMING → hamming_distance
 *   MDB_ANN_CANDIDATE_COSINE  → cosine_distance (1 - cosine_similarity)
 * Dense cosine is never encoded as a Hamming integer.
 */
typedef struct {
    uint64_t row_id;
    int32_t candidate_distance_kind; /* mongreldb_ann_candidate_distance_kind */
    uint32_t hamming_distance;
    float cosine_distance;
    float exact_score;
} mongreldb_ann_rerank_hit;

typedef struct {
    uint16_t column_id;
    mongreldb_value value;
} mongreldb_cell_input;

typedef struct {
    const mongreldb_cell_input *data;
    size_t len;
} mongreldb_cell_input_array;

typedef struct {
    const mongreldb_cell_input_array *data;
    size_t len;
} mongreldb_row_input_array;


/* ── Hybrid search request structs ──────────────────────────────────────── */

typedef enum {
    MDB_RETRIEVER_ANN      = 0,
    MDB_RETRIEVER_SPARSE   = 1,
    MDB_RETRIEVER_MIN_HASH = 2,
} mongreldb_retriever_kind;

typedef enum {
    MDB_SEARCH_METRIC_COSINE    = 0,
    MDB_SEARCH_METRIC_DOT       = 1,
    MDB_SEARCH_METRIC_EUCLIDEAN = 2,
} mongreldb_search_metric;

typedef enum {
    MDB_FUSION_RECIPROCAL_RANK = 0,
} mongreldb_fusion_kind;

typedef struct {
    int32_t kind;
    uint16_t column_id;
    const char *name;
    double weight;
    uint32_t k;
    mongreldb_embedding_view embedding;
    mongreldb_sparse_term_array sparse_terms;
    mongreldb_minhash_members minhash_members;
} mongreldb_retriever;

typedef struct {
    const mongreldb_retriever *data;
    size_t len;
} mongreldb_retriever_array;

typedef struct {
    int32_t kind;
    uint32_t reciprocal_rank_constant;
} mongreldb_fusion;

typedef struct {
    int32_t kind;
    uint16_t embedding_column;
    mongreldb_embedding_view query;
    int32_t metric;
    uint32_t candidate_limit;
    double weight;
} mongreldb_rerank;

typedef struct {
    const mongreldb_condition *data;
    size_t len;
} mongreldb_condition_array;

typedef struct {
    const uint16_t *data;
    size_t len;
} mongreldb_projection;

typedef struct {
    mongreldb_condition_array must;
    mongreldb_retriever_array retrievers;
    mongreldb_fusion fusion;
    const mongreldb_rerank *rerank;
    size_t limit;
    mongreldb_projection projection;
} mongreldb_search_request;

/* ── Error accessors ────────────────────────────────────────────────────── */

const char *mongreldb_last_error(void);
int32_t mongreldb_last_error_code(void);

/* Structured error metadata. Text fields are always NUL-terminated. Check
 * outcome_known before treating committed=0 as proof that no write occurred.
 * The struct is copied from thread-local storage and remains owned by caller. */
typedef struct mongreldb_error_details_v1 {
    size_t struct_size;
    uint32_t version;
    int32_t code;
    uint8_t outcome_known;
    uint8_t committed;
    uint8_t retryable;
    uint8_t has_last_commit_epoch;
    uint64_t last_commit_epoch;
    size_t committed_statements;
    uint8_t has_first_commit_statement_index;
    size_t first_commit_statement_index;
    uint8_t has_last_commit_statement_index;
    size_t last_commit_statement_index;
    size_t completed_statements;
    uint8_t has_statement_index;
    size_t statement_index;
    int32_t cancel_outcome;
    int32_t cancellation_reason;
    char query_id[33];
    char server_state[32];
} mongreldb_error_details_v1;

int32_t mongreldb_last_error_details_v1(
    mongreldb_error_details_v1 *out_details);
void mongreldb_free_error_string(char *ptr);
void mongreldb_free_string(char *ptr);

/** Runtime build identity JSON. Free with mongreldb_free_string(). */
char *mongreldb_build_info(void);

/* ── Database lifecycle ─────────────────────────────────────────────────── */

mongreldb_database_t *mongreldb_create(const char *path);
mongreldb_database_t *mongreldb_open(const char *path);
mongreldb_database_t *mongreldb_create_with_credentials(
    const char *path, const char *username, const char *password);
mongreldb_database_t *mongreldb_open_with_credentials(
    const char *path, const char *username, const char *password);
/** AES-256-GCM encrypted create/open (passphrase → KEK). */
mongreldb_database_t *mongreldb_create_encrypted(
    const char *path, const char *passphrase);
mongreldb_database_t *mongreldb_open_encrypted(
    const char *path, const char *passphrase);
/** Encrypted + require_auth bootstrap / open (passphrase + admin user). */
mongreldb_database_t *mongreldb_create_encrypted_with_credentials(
    const char *path, const char *passphrase,
    const char *username, const char *password);
mongreldb_database_t *mongreldb_open_encrypted_with_credentials(
    const char *path, const char *passphrase,
    const char *username, const char *password);
int32_t mongreldb_database_close(mongreldb_database_t *db);
void mongreldb_database_free(mongreldb_database_t *db);
int32_t mongreldb_database_compact(mongreldb_database_t *db);
int32_t mongreldb_database_compact_table(mongreldb_database_t *db, const char *name);
int32_t mongreldb_database_table_names(
    mongreldb_database_t *db,
    char ***out_names, size_t *out_count);

/* ── DDL ────────────────────────────────────────────────────────────────── */

int32_t mongreldb_create_table(
    mongreldb_database_t *db, const char *name,
    mongreldb_schema_t *schema, uint64_t *out_table_id);
int32_t mongreldb_drop_table(mongreldb_database_t *db, const char *name);
int32_t mongreldb_rename_table(
    mongreldb_database_t *db, const char *name, const char *new_name);

/* ── SQL execution ──────────────────────────────────────────────────────── */
/*
 * Run a SQL statement via the DataFusion engine (same engine the daemon and
 * Kit use). Results are returned as Arrow IPC *file* bytes (the format Arrow
 * calls "IPC file" or "Feather v2" - starts with the 6-byte "ARROW1" magic).
 * DDL/DML statements that produce no rows return a zero-length buffer
 * (*out_len = 0). Decode with any Arrow IPC file reader (e.g. the C++
 * arrow::ipc::RecordBatchFileReader, nanoarrow, or pyarrow.ipc.open_file).
 *
 * The session is cached on the database handle so repeated calls reuse
 * catalog/view state. After a table create/drop via the FFI (not via SQL),
 * call mongreldb_database_sql_refresh() so the session sees the new tables.
 *
 * The caller owns *out_buf and must free it with mongreldb_free_sql_result().
 * On error, *out_buf is unchanged and a negative code is returned (call
 * mongreldb_last_error() for details).
 */
int32_t mongreldb_database_sql(
    mongreldb_database_t *db, const char *sql,
    uint8_t **out_buf, size_t *out_len);

typedef struct mongreldb_sql_options {
    const char *query_id;
    uint64_t timeout_ms;
} mongreldb_sql_options;

typedef struct mongreldb_sql_options_v2 {
    const char *query_id;
    uint64_t timeout_ms;
    size_t max_output_rows;
    size_t max_output_bytes;
} mongreldb_sql_options_v2;

typedef struct mongreldb_sql_result_t {
    uint8_t *data;
    size_t len;
} mongreldb_sql_result_t;

mongreldb_sql_query_t *mongreldb_sql_query_start(
    mongreldb_database_t *db,
    const char *sql,
    const mongreldb_sql_options *options);

/* Versioned start API. Zero limits select the defaults: 1,000,000 rows and
 * 64 MiB. */
mongreldb_sql_query_t *mongreldb_sql_query_start_v2(
    mongreldb_database_t *db,
    const char *sql,
    const mongreldb_sql_options_v2 *options);

typedef enum mongreldb_cancel_outcome {
    MDB_CANCEL_ACCEPTED = 1,
    MDB_CANCEL_ALREADY_CANCELLING = 2,
    MDB_CANCEL_TOO_LATE = 3,
    MDB_CANCEL_ALREADY_FINISHED = 4,
    MDB_CANCEL_NOT_FOUND = 5
} mongreldb_cancel_outcome;

typedef enum mongreldb_sql_query_phase {
    MDB_SQL_PHASE_NONE = 0,
    MDB_SQL_PHASE_QUEUED = 1,
    MDB_SQL_PHASE_PLANNING = 2,
    MDB_SQL_PHASE_EXECUTING = 3,
    MDB_SQL_PHASE_STREAMING = 4,
    MDB_SQL_PHASE_SERIALIZING = 5,
    MDB_SQL_PHASE_COMMIT_CRITICAL = 6,
    MDB_SQL_PHASE_CANCELLING = 7,
    MDB_SQL_PHASE_COMPLETED = 8,
    MDB_SQL_PHASE_FAILED = 9,
    MDB_SQL_PHASE_CANCELLED = 10
} mongreldb_sql_query_phase;

typedef enum mongreldb_sql_terminal_state {
    MDB_SQL_TERMINAL_NONE = 0,
    MDB_SQL_TERMINAL_COMPLETED = 1,
    MDB_SQL_TERMINAL_FAILED_BEFORE_COMMIT = 2,
    MDB_SQL_TERMINAL_CANCELLED_BEFORE_COMMIT = 3,
    MDB_SQL_TERMINAL_DEADLINE_BEFORE_COMMIT = 4,
    MDB_SQL_TERMINAL_COMMITTED = 5,
    MDB_SQL_TERMINAL_COMMITTED_WITH_ERROR = 6,
    MDB_SQL_TERMINAL_PARTIALLY_COMMITTED = 7,
    MDB_SQL_TERMINAL_CANCELLED_AFTER_COMMIT = 8,
    MDB_SQL_TERMINAL_DEADLINE_AFTER_COMMIT = 9,
    MDB_SQL_TERMINAL_OUTCOME_UNKNOWN = 10
} mongreldb_sql_terminal_state;

typedef enum mongreldb_cancellation_reason {
    MDB_CANCEL_REASON_NONE = 0,
    MDB_CANCEL_REASON_CLIENT_REQUEST = 1,
    MDB_CANCEL_REASON_DEADLINE = 2,
    MDB_CANCEL_REASON_CLIENT_DISCONNECTED = 3,
    MDB_CANCEL_REASON_SESSION_CLOSED = 4,
    MDB_CANCEL_REASON_SERVER_SHUTDOWN = 5
} mongreldb_cancellation_reason;

typedef enum mongreldb_sql_terminal_error_category {
    MDB_SQL_ERROR_CATEGORY_NONE = 0,
    MDB_SQL_ERROR_CATEGORY_CANCELLATION = 1,
    MDB_SQL_ERROR_CATEGORY_DEADLINE = 2,
    MDB_SQL_ERROR_CATEGORY_RESULT_LIMIT = 3,
    MDB_SQL_ERROR_CATEGORY_SERIALIZATION = 4,
    MDB_SQL_ERROR_CATEGORY_EXECUTION = 5
} mongreldb_sql_terminal_error_category;

typedef struct mongreldb_sql_query_status_v1 {
    char query_id[33];
    int32_t phase;
    int32_t terminal_state;
    uint8_t committed;
    size_t committed_statements;
    uint8_t has_last_commit_epoch;
    uint64_t last_commit_epoch;
    uint8_t has_first_commit_statement_index;
    size_t first_commit_statement_index;
    uint8_t has_last_commit_statement_index;
    size_t last_commit_statement_index;
    size_t completed_statements;
    size_t statement_index;
    int32_t cancel_outcome;
    int32_t cancellation_reason;
    uint8_t retryable;
    int32_t terminal_error_category;
    char terminal_error_code[64];
} mongreldb_sql_query_status_v1;

/* V1 ABI remains supported. If terminal_state is
 * MDB_SQL_TERMINAL_OUTCOME_UNKNOWN, its commit/progress fields are legacy
 * zero placeholders and do not prove that no commit occurred. */

typedef struct mongreldb_sql_query_status_v2 {
    mongreldb_sql_query_status_v1 v1;
    /* 1 when v1 commit/progress fields are known; 0 means ignore them. */
    uint8_t outcome_known;
} mongreldb_sql_query_status_v2;

/* Returns one mongreldb_cancel_outcome value, or a negative error code on bad
 * input. MDB_CANCEL_TOO_LATE means durable commit already won. */
int32_t mongreldb_sql_query_cancel(mongreldb_sql_query_t *query);

/* Structured query state. Text fields are always NUL-terminated. */
int32_t mongreldb_sql_query_get_status(
    mongreldb_sql_query_t *query,
    mongreldb_sql_query_status_v1 *out_status);

/* Preferred structured status API. Check outcome_known before reading any
 * v1 commit/progress field. */
int32_t mongreldb_sql_query_get_status_v2(
    mongreldb_sql_query_t *query,
    mongreldb_sql_query_status_v2 *out_status);

/* Wait returns stable SQL-specific error codes. In particular,
 * MDB_ERR_QUERY_CANCELLED_AFTER_COMMIT,
 * MDB_ERR_DEADLINE_AFTER_COMMIT, and
 * MDB_ERR_SERIALIZATION_AFTER_COMMIT preserve durable-outcome semantics.
 * Call mongreldb_sql_query_get_status_v2 for the full receipt fields. */
int32_t mongreldb_sql_query_wait(
    mongreldb_sql_query_t *query,
    mongreldb_sql_result_t *out_result);

void mongreldb_sql_query_free(mongreldb_sql_query_t *query);

/* Rebuild the cached SQL session so it sees the current table set after a
 * schema change made outside SQL (e.g. via mongreldb_create_table). Returns
 * MDB_OK on success. */
int32_t mongreldb_database_sql_refresh(mongreldb_database_t *db);

/* Free a byte buffer returned by mongreldb_database_sql(). Safe to call with
 * NULL or a zero-length buffer. */
void mongreldb_free_sql_result(uint8_t *ptr, size_t len);

/* ── Migration planning ─────────────────────────────────────────────────── */
/*
 * Migration *planning* and *checksum* logic is centralized in the FFI so every
 * language binding produces identical results. The *execution* (applying each
 * MigrationOp to a live database) is done by the host language using the
 * existing FFI functions (mongreldb_create_table, mongreldb_database_sql, etc.).
 *
 * All functions use JSON for the complex Migration/MigrationOp types:
 *   Migration: {"version":1,"name":"initial","ops":[...]}
 *   MigrationOp variants: see docs/migrations.md for the full list.
 */

/* Plan pending migrations. Takes applied_json (migrations already in the db)
 * and desired_json (the full app-defined ordered set), returns a JSON array
 * of pending migrations (version > max applied), sorted by version. The
 * result is written to *out_json (NUL-terminated UTF-8, caller frees with
 * mongreldb_free_migrate_string). Returns MDB_OK on success. */
int32_t mongreldb_plan_migrations_json(
    const char *applied_json,
    const char *desired_json,
    const char **out_json);

/* Compute the canonical SHA-256 checksum for a single migration (identical
 * across all language bindings). Takes version, name, and ops_json (a JSON
 * array of MigrationOp). Result is a 64-char hex string written to
 * *out_checksum (caller frees with mongreldb_free_migrate_string). */
int32_t mongreldb_migration_checksum_json(
    int64_t version,
    const char *name,
    const char *ops_json,
    const char **out_checksum);

/* Free a string returned by the migration functions above. */
void mongreldb_free_migrate_string(char *ptr);

/* ── Schema builder ─────────────────────────────────────────────────────── */

mongreldb_schema_builder_t *mongreldb_schema_begin(void);
int32_t mongreldb_schema_add_column(
    mongreldb_schema_builder_t *builder, const mongreldb_column_def *col);
int32_t mongreldb_schema_add_index(
    mongreldb_schema_builder_t *builder, const mongreldb_index_def *idx);
int32_t mongreldb_schema_add_unique(
    mongreldb_schema_builder_t *builder, const mongreldb_unique_constraint *uc);
int32_t mongreldb_schema_add_foreign_key(
    mongreldb_schema_builder_t *builder, const mongreldb_foreign_key *fk);
int32_t mongreldb_schema_set_clustered(
    mongreldb_schema_builder_t *builder, bool clustered);
mongreldb_schema_t *mongreldb_schema_build(mongreldb_schema_builder_t *builder);
void mongreldb_schema_free(mongreldb_schema_t *schema);
void mongreldb_schema_builder_free(mongreldb_schema_builder_t *builder);

/* ── Table operations ───────────────────────────────────────────────────── */

mongreldb_table_t *mongreldb_database_table(
    mongreldb_database_t *db, const char *name);
void mongreldb_table_free(mongreldb_table_t *t);
int32_t mongreldb_table_put(
    mongreldb_table_t *t, const mongreldb_cell_input_array *cells,
    uint64_t *out_row_id);
int32_t mongreldb_table_put_batch(
    mongreldb_table_t *t, const mongreldb_row_input_array *rows);
int32_t mongreldb_table_count(mongreldb_table_t *t, uint64_t *out_count);
int32_t mongreldb_table_delete(mongreldb_table_t *t, uint64_t row_id);

/* ── Query builder ──────────────────────────────────────────────────────── */

int32_t mongreldb_minhash_member_hash_v1_json(const char *member, uint64_t *out_hash);
mongreldb_query_t *mongreldb_query_begin(void);
int32_t mongreldb_query_add(
    mongreldb_query_t *q, const mongreldb_condition *cond);
int32_t mongreldb_query_set_projection(
    mongreldb_query_t *q, const uint16_t *cols, size_t len);
int32_t mongreldb_query_set_limit(mongreldb_query_t *q, uint64_t limit);
int32_t mongreldb_query_set_offset(mongreldb_query_t *q, uint64_t offset);
mongreldb_query_t *mongreldb_query_build(mongreldb_query_t *q);
void mongreldb_query_free(mongreldb_query_t *q);

/* ── Query execution + result iteration ─────────────────────────────────── */

mongreldb_result_t *mongreldb_table_query(
    mongreldb_table_t *t, mongreldb_query_t *q);
size_t mongreldb_result_count(mongreldb_result_t *r);
int32_t mongreldb_result_row(mongreldb_result_t *r, size_t index, mongreldb_row *out_row);
size_t mongreldb_row_cell_count(const mongreldb_row *row);
int32_t mongreldb_row_cell(
    const mongreldb_row *row, size_t index, mongreldb_cell *out_cell);
void mongreldb_result_free(mongreldb_result_t *r);

mongreldb_ann_rerank_result_t *mongreldb_table_ann_rerank(
    mongreldb_table_t *t, uint16_t column_id,
    mongreldb_embedding_view query, size_t candidate_k, size_t limit,
    int32_t metric);
size_t mongreldb_ann_rerank_result_count(mongreldb_ann_rerank_result_t *r);
int32_t mongreldb_ann_rerank_result_hit(
    mongreldb_ann_rerank_result_t *r, size_t index,
    mongreldb_ann_rerank_hit *out_hit);
void mongreldb_ann_rerank_result_free(mongreldb_ann_rerank_result_t *r);


/* ── Hybrid search ─────────────────────────────────────────────────────── */

/* Optional opaque builder handle (empty request). Preferred path is to fill a
 * stack mongreldb_search_request and pass it to mongreldb_table_search. */
mongreldb_search_request_t *mongreldb_search_request_begin(void);
void mongreldb_search_request_free(mongreldb_search_request_t *req);
mongreldb_result_t *mongreldb_table_search(
    mongreldb_table_t *t, const mongreldb_search_request *req);

/* ── Transaction (staging buffer) ───────────────────────────────────────── */

mongreldb_transaction_t *mongreldb_begin(mongreldb_database_t *db);
void mongreldb_txn_free(mongreldb_transaction_t *txn);
int32_t mongreldb_txn_rollback(mongreldb_transaction_t *txn);
int32_t mongreldb_txn_put(
    mongreldb_transaction_t *txn, const char *table,
    const mongreldb_cell_input_array *cells);
int32_t mongreldb_txn_upsert(
    mongreldb_transaction_t *txn, const char *table,
    const mongreldb_cell_input_array *cells,
    const mongreldb_cell_input_array *update_cells);
int32_t mongreldb_txn_delete(
    mongreldb_transaction_t *txn, const char *table, uint64_t row_id);
int32_t mongreldb_txn_delete_by_pk(
    mongreldb_transaction_t *txn, const char *table, const mongreldb_value *pk);
int32_t mongreldb_txn_commit(
    mongreldb_transaction_t *txn, uint64_t *out_epoch);

/* ── Auth: users ────────────────────────────────────────────────────────── */

int32_t mongreldb_create_user(
    mongreldb_database_t *db, const char *username, const char *password);
int32_t mongreldb_drop_user(mongreldb_database_t *db, const char *username);
int32_t mongreldb_alter_user_password(
    mongreldb_database_t *db, const char *username, const char *new_password);
int32_t mongreldb_verify_user(
    mongreldb_database_t *db, const char *username, const char *password,
    uint8_t *out_ok);
int32_t mongreldb_set_user_admin(
    mongreldb_database_t *db, const char *username, bool is_admin);

/* ── Auth: roles ────────────────────────────────────────────────────────── */

int32_t mongreldb_create_role(mongreldb_database_t *db, const char *name);
int32_t mongreldb_drop_role(mongreldb_database_t *db, const char *name);
int32_t mongreldb_grant_role(
    mongreldb_database_t *db, const char *username, const char *role_name);
int32_t mongreldb_revoke_role(
    mongreldb_database_t *db, const char *username, const char *role_name);
int32_t mongreldb_grant_permission(
    mongreldb_database_t *db, const char *role_name, const char *permission);
int32_t mongreldb_revoke_permission(
    mongreldb_database_t *db, const char *role_name, const char *permission);
int32_t mongreldb_enable_auth(
    mongreldb_database_t *db, const char *admin_username, const char *admin_password);
int32_t mongreldb_disable_auth(mongreldb_database_t *db);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* MONGRELDB_H */
