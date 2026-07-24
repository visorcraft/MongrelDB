/*
 * C smoke test for the mongreldb-ffi C ABI.
 *
 * Compiled by the `cc` crate in tests/c_smoke_test.rs and linked against
 * libmongreldb_ffi. Exercises the full create → schema → put → query →
 * transaction → auth lifecycle through pure C.
 */
#include "mongreldb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define CHECK(call) do { \
    int32_t _rc = (call); \
    if (_rc != MDB_OK) { \
        fprintf(stderr, "FAIL: %s returned %d: %s\n", #call, _rc, mongreldb_last_error()); \
        exit(1); \
    } \
} while(0)

static char test_db_dir[] = "/tmp/mdb_c_test_XXXXXX";

static mongreldb_database_t *create_test_db(void) {
    char *dir = mkdtemp(test_db_dir);
    if (!dir) { perror("mkdtemp"); exit(1); }
    mongreldb_database_t *db = mongreldb_create(dir);
    if (!db) {
        fprintf(stderr, "create failed: %s\n", mongreldb_last_error());
        exit(1);
    }
    return db;
}

static void make_simple_table(mongreldb_database_t *db, const char *name) {
    mongreldb_schema_builder_t *builder = mongreldb_schema_begin();

    mongreldb_column_def col1 = {
        .id = 1, .name = "id", .ty = MDB_TYPE_INT64,
        .flags = MDB_COL_PRIMARY_KEY, .embedding_dim = 0,
        .decimal_precision = 0, .decimal_scale = 0,
    };
    mongreldb_string_array empty_strings = {0};
    col1.enum_variants = empty_strings;
    CHECK(mongreldb_schema_add_column(builder, &col1));

    mongreldb_column_def col2 = {
        .id = 2, .name = "name", .ty = MDB_TYPE_BYTES,
        .flags = 0, .embedding_dim = 0,
        .decimal_precision = 0, .decimal_scale = 0,
    };
    col2.enum_variants = empty_strings;
    CHECK(mongreldb_schema_add_column(builder, &col2));

    mongreldb_schema_t *schema = mongreldb_schema_build(builder);
    assert(schema);
    mongreldb_schema_builder_free(builder);

    uint64_t table_id;
    CHECK(mongreldb_create_table(db, name, schema, &table_id));
    /* create_table consumes the schema handle — do NOT free it. */
}

static void expect_invalid(int32_t rc, const char *message) {
    assert(rc == MDB_ERR_INVALID_ARGUMENT);
    assert(mongreldb_last_error_code() == MDB_ERR_INVALID_ARGUMENT);
    assert(strcmp(mongreldb_last_error(), message) == 0);
    mongreldb_error_details_v1 details = {0};
    assert(mongreldb_last_error_details_v1(&details) == MDB_OK);
    assert(details.struct_size == sizeof(details));
    assert(details.version == 1);
    assert(details.code == MDB_ERR_INVALID_ARGUMENT);
    assert(details.outcome_known == 1);
    assert(details.committed == 0);
    /* FND-007: InvalidArgument → ClusterVersionMismatch (code 17). */
    assert(mongreldb_last_error_category_code() == 17);
    assert(details.category_code == 17);
    assert(strcmp(mongreldb_last_error_category(), "cluster version mismatch") == 0);
}

static void test_invalid_discriminants(mongreldb_database_t *db) {
    mongreldb_schema_builder_t *builder = mongreldb_schema_begin();
    mongreldb_column_def bad_column = {
        .id = 9, .name = "bad", .ty = 99,
    };
    expect_invalid(
        mongreldb_schema_add_column(builder, &bad_column),
        "invalid type id 99");

    mongreldb_index_def bad_index = {
        .name = "bad_idx", .column_id = 1, .kind = 99,
    };
    expect_invalid(
        mongreldb_schema_add_index(builder, &bad_index),
        "invalid index kind 99");

    mongreldb_foreign_key bad_fk = {
        .id = 1, .name = "bad_fk", .ref_table = "items",
        .on_delete = 99, .on_update = MDB_FK_RESTRICT,
    };
    expect_invalid(
        mongreldb_schema_add_foreign_key(builder, &bad_fk),
        "invalid foreign key action 99");
    mongreldb_schema_builder_free(builder);

    mongreldb_table_t *table = mongreldb_database_table(db, "items");
    assert(table);
    mongreldb_cell_input bad_cell = { .column_id = 1 };
    bad_cell.value.tag = 99;
    mongreldb_cell_input_array bad_cells = { .data = &bad_cell, .len = 1 };
    expect_invalid(
        mongreldb_table_put(table, &bad_cells, NULL),
        "invalid value tag 99");

    mongreldb_query_t *query = mongreldb_query_begin();
    mongreldb_condition bad_condition = { .kind = 99 };
    expect_invalid(
        mongreldb_query_add(query, &bad_condition),
        "invalid condition kind 99");
    mongreldb_query_free(query);
    mongreldb_table_free(table);
}

static int32_t put_secure_row(
    mongreldb_table_t *table,
    int64_t id,
    const char *owner,
    const char *secret,
    const float embedding[2],
    uint64_t *row_id) {
    mongreldb_cell_input cells[4] = {0};
    cells[0].column_id = 1;
    cells[0].value.tag = MDB_VALUE_INT64;
    cells[0].value.v.i64 = id;
    cells[1].column_id = 2;
    cells[1].value.tag = MDB_VALUE_BYTES;
    cells[1].value.v.bytes.data = (const uint8_t *)owner;
    cells[1].value.v.bytes.len = strlen(owner);
    cells[2].column_id = 3;
    cells[2].value.tag = MDB_VALUE_BYTES;
    cells[2].value.v.bytes.data = (const uint8_t *)secret;
    cells[2].value.v.bytes.len = strlen(secret);
    cells[3].column_id = 4;
    cells[3].value.tag = MDB_VALUE_EMBEDDING;
    cells[3].value.v.embedding.data = embedding;
    cells[3].value.v.embedding.len = 2;
    mongreldb_cell_input_array input = { .data = cells, .len = 4 };
    return mongreldb_table_put(table, &input, row_id);
}

static mongreldb_result_t *ann_query(mongreldb_table_t *table, const float embedding[2]) {
    mongreldb_query_t *query = mongreldb_query_begin();
    mongreldb_condition condition = {0};
    condition.kind = MDB_COND_ANN;
    condition.column_id = 4;
    condition.k = 2;
    condition.embedding.data = embedding;
    condition.embedding.len = 2;
    CHECK(mongreldb_query_add(query, &condition));
    mongreldb_result_t *result = mongreldb_table_query(table, query);
    mongreldb_query_free(query);
    return result;
}

static void test_authenticated_native_reads(void) {
    char tmpl[] = "/tmp/mdb_c_auth_test_XXXXXX";
    char *dir = mkdtemp(tmpl);
    if (!dir) { perror("mkdtemp"); exit(1); }

    mongreldb_database_t *admin =
        mongreldb_create_with_credentials(dir, "admin", "admin-pw");
    assert(admin);

    mongreldb_schema_builder_t *builder = mongreldb_schema_begin();
    mongreldb_string_array no_variants = {0};
    mongreldb_column_def columns[] = {
        { .id = 1, .name = "id", .ty = MDB_TYPE_INT64,
          .flags = MDB_COL_PRIMARY_KEY, .enum_variants = no_variants },
        { .id = 2, .name = "owner", .ty = MDB_TYPE_BYTES,
          .enum_variants = no_variants },
        { .id = 3, .name = "secret", .ty = MDB_TYPE_BYTES,
          .enum_variants = no_variants },
        { .id = 4, .name = "embedding", .ty = MDB_TYPE_EMBEDDING,
          .flags = MDB_COL_EMBEDDING_BINARY_QUANTIZED,
          .embedding_dim = 2, .enum_variants = no_variants },
    };
    for (size_t i = 0; i < sizeof(columns) / sizeof(columns[0]); i++) {
        CHECK(mongreldb_schema_add_column(builder, &columns[i]));
    }
    CHECK(mongreldb_schema_set_embedding_source_json(
        builder, 4, "{\"kind\":\"supplied_by_application\"}"));
    mongreldb_index_def index = {
        .name = "ann_idx", .column_id = 4, .kind = MDB_INDEX_ANN,
    };
    mongreldb_index_options_v1 index_options = {
        .struct_size = sizeof(mongreldb_index_options_v1),
        .version = 1,
        .ann_m = 24,
        .ann_ef_construction = 96,
        .ann_ef_search = 48,
        .ann_quantization = MDB_ANN_QUANTIZATION_DENSE,
    };
    CHECK(mongreldb_schema_add_index_v2(builder, &index, &index_options));
    mongreldb_schema_t *schema = mongreldb_schema_build(builder);
    assert(schema);
    mongreldb_schema_builder_free(builder);
    uint64_t table_id = 0;
    CHECK(mongreldb_create_table(admin, "docs", schema, &table_id));

    mongreldb_table_t *admin_table = mongreldb_database_table(admin, "docs");
    assert(admin_table);
    const float alice_embedding[2] = {0.0f, 1.0f};
    const float bob_embedding[2] = {1.0f, 0.0f};
    uint64_t alice_row_id = 0;
    uint64_t bob_row_id = 0;
    CHECK(put_secure_row(admin_table, 1, "alice", "alice-secret", alice_embedding, &alice_row_id));
    CHECK(put_secure_row(admin_table, 2, "bob", "bob-secret", bob_embedding, &bob_row_id));
    assert(alice_row_id != bob_row_id);

    CHECK(mongreldb_create_user(admin, "alice", "alice-pw"));
    CHECK(mongreldb_create_role(admin, "reader"));
    CHECK(mongreldb_grant_permission(admin, "reader", "select:docs"));
    CHECK(mongreldb_grant_permission(admin, "reader", "insert:docs"));
    CHECK(mongreldb_grant_permission(admin, "reader", "delete:docs"));
    CHECK(mongreldb_grant_role(admin, "alice", "reader"));
    CHECK(mongreldb_database_sql_refresh(admin));
    uint8_t *sql_buf = NULL;
    size_t sql_len = 0;
    CHECK(mongreldb_database_sql(admin,
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY", &sql_buf, &sql_len));
    mongreldb_free_sql_result(sql_buf, sql_len);
    CHECK(mongreldb_database_sql(admin,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
        &sql_buf, &sql_len));
    mongreldb_free_sql_result(sql_buf, sql_len);
    CHECK(mongreldb_database_sql(admin,
        "CREATE MASK hide_secret ON docs(secret) USING REDACT '***'",
        &sql_buf, &sql_len));
    mongreldb_free_sql_result(sql_buf, sql_len);
    mongreldb_table_free(admin_table);
    mongreldb_database_free(admin);

    mongreldb_database_t *alice =
        mongreldb_open_with_credentials(dir, "alice", "alice-pw");
    assert(alice);
    mongreldb_table_t *alice_table = mongreldb_database_table(alice, "docs");
    assert(alice_table);

    mongreldb_result_t *result = ann_query(alice_table, bob_embedding);
    assert(result);
    assert(mongreldb_result_count(result) == 1);
    mongreldb_row row = {0};
    CHECK(mongreldb_result_row(result, 0, &row));
    int saw_owner = 0;
    int saw_mask = 0;
    for (size_t i = 0; i < mongreldb_row_cell_count(&row); i++) {
        mongreldb_cell cell = {0};
        CHECK(mongreldb_row_cell(&row, i, &cell));
        if (cell.column_id == 2) {
            saw_owner = cell.value.tag == MDB_VALUE_BYTES &&
                cell.value.v.bytes.len == 5 &&
                memcmp(cell.value.v.bytes.data, "alice", 5) == 0;
        }
        if (cell.column_id == 3) {
            saw_mask = cell.value.tag == MDB_VALUE_BYTES &&
                cell.value.v.bytes.len == 3 &&
                memcmp(cell.value.v.bytes.data, "***", 3) == 0;
        }
    }
    assert(saw_owner && saw_mask);
    mongreldb_result_free(result);

    mongreldb_ann_rerank_result_t *reranked = mongreldb_table_ann_rerank(
        alice_table, 4,
        (mongreldb_embedding_view){ .data = bob_embedding, .len = 2 },
        2, 1, MDB_VECTOR_COSINE);
    assert(reranked);
    assert(mongreldb_ann_rerank_result_count(reranked) == 1);
    mongreldb_ann_rerank_result_free(reranked);

    reranked = mongreldb_table_ann_rerank(
        alice_table, 4,
        (mongreldb_embedding_view){ .data = bob_embedding, .len = 2 },
        2, 1, 99);
    assert(reranked == NULL);
    assert(mongreldb_last_error_code() == MDB_ERR_INVALID_ARGUMENT);
    assert(strcmp(mongreldb_last_error(),
        "invalid vector metric 99; expected 0, 1, or 2") == 0);

    assert(mongreldb_table_delete(alice_table, bob_row_id) == MDB_ERR_UNAUTHORIZED);
    mongreldb_transaction_t *forbidden_update = mongreldb_begin(alice);
    assert(forbidden_update);
    mongreldb_cell_input bob_cells[4] = {0};
    bob_cells[0].column_id = 1;
    bob_cells[0].value.tag = MDB_VALUE_INT64;
    bob_cells[0].value.v.i64 = 2;
    bob_cells[1].column_id = 2;
    bob_cells[1].value.tag = MDB_VALUE_BYTES;
    bob_cells[1].value.v.bytes.data = (const uint8_t *)"bob";
    bob_cells[1].value.v.bytes.len = 3;
    bob_cells[2].column_id = 3;
    bob_cells[2].value.tag = MDB_VALUE_BYTES;
    bob_cells[2].value.v.bytes.data = (const uint8_t *)"bob-secret";
    bob_cells[2].value.v.bytes.len = 10;
    bob_cells[3].column_id = 4;
    bob_cells[3].value.tag = MDB_VALUE_EMBEDDING;
    bob_cells[3].value.v.embedding.data = bob_embedding;
    bob_cells[3].value.v.embedding.len = 2;
    mongreldb_cell_input update_cell = {0};
    update_cell.column_id = 3;
    update_cell.value.tag = MDB_VALUE_BYTES;
    update_cell.value.v.bytes.data = (const uint8_t *)"stolen";
    update_cell.value.v.bytes.len = 6;
    mongreldb_cell_input_array bob_input = { .data = bob_cells, .len = 4 };
    mongreldb_cell_input_array update_input = { .data = &update_cell, .len = 1 };
    CHECK(mongreldb_txn_upsert(forbidden_update, "docs", &bob_input, &update_input));
    uint64_t forbidden_epoch = 0;
    assert(mongreldb_txn_commit(forbidden_update, &forbidden_epoch) == MDB_ERR_UNAUTHORIZED);
    mongreldb_txn_free(forbidden_update);
    assert(put_secure_row(alice_table, 3, "bob", "forbidden", bob_embedding, NULL) ==
        MDB_ERR_UNAUTHORIZED);
    CHECK(put_secure_row(alice_table, 3, "alice", "allowed", alice_embedding, NULL));
    mongreldb_table_free(alice_table);
    mongreldb_database_free(alice);

    admin = mongreldb_open_with_credentials(dir, "admin", "admin-pw");
    assert(admin);
    admin_table = mongreldb_database_table(admin, "docs");
    assert(admin_table);
    result = ann_query(admin_table, bob_embedding);
    assert(result && mongreldb_result_count(result) == 2);
    mongreldb_result_free(result);

    CHECK(mongreldb_revoke_role(admin, "alice", "reader"));
    mongreldb_table_free(admin_table);
    mongreldb_database_free(admin);

    alice = mongreldb_open_with_credentials(dir, "alice", "alice-pw");
    assert(alice);
    alice_table = mongreldb_database_table(alice, "docs");
    assert(alice_table);
    result = ann_query(alice_table, bob_embedding);
    assert(result == NULL);
    assert(mongreldb_last_error_code() == MDB_ERR_UNAUTHORIZED);
    assert(put_secure_row(alice_table, 4, "alice", "revoked", alice_embedding, NULL) ==
        MDB_ERR_UNAUTHORIZED);

    mongreldb_table_free(alice_table);
    mongreldb_database_free(alice);
}

int main(void) {
    char *build_info = mongreldb_build_info();
    assert(build_info != NULL);
    /* Version is checked against CARGO_PKG_VERSION of the linked library
     * after bump-version; print the payload so CI mismatches are obvious. */
    if (strstr(build_info, "\"engine_version\":\"0.64.7\"") == NULL ||
        strstr(build_info, "\"query_version\":\"0.64.7\"") == NULL) {
        fprintf(stderr, "unexpected mongreldb_build_info: %s\n", build_info);
        mongreldb_free_string(build_info);
        return 1;
    }
    mongreldb_free_string(build_info);

    /* ── Database lifecycle ──────────────────────────────────────────── */
    mongreldb_database_t *db = create_test_db();
    printf("1. database created\n");

    /* ── Schema + table ──────────────────────────────────────────────── */
    make_simple_table(db, "items");
    printf("2. table created\n");
    test_invalid_discriminants(db);
    printf("2a. invalid discriminants rejected\n");

    /* ── Table put ───────────────────────────────────────────────────── */
    mongreldb_table_t *table = mongreldb_database_table(db, "items");
    assert(table);

    mongreldb_cell_input cells[2] = {0};
    cells[0].column_id = 1;
    cells[0].value.tag = MDB_VALUE_INT64;
    cells[0].value.v.i64 = 42;

    cells[1].column_id = 2;
    cells[1].value.tag = MDB_VALUE_BYTES;
    cells[1].value.v.bytes.data = (const uint8_t *)"hello";
    cells[1].value.v.bytes.len = 5;

    mongreldb_cell_input_array cell_arr = {
        .data = cells, .len = 2,
    };

    uint64_t row_id;
    CHECK(mongreldb_table_put(table, &cell_arr, &row_id));
    assert(row_id != 42);
    cells[0].value.v.i64 = 43;
    cells[1].value.v.bytes.data = (const uint8_t *)"world";
    uint64_t second_row_id;
    CHECK(mongreldb_table_put(table, &cell_arr, &second_row_id));
    assert(second_row_id != row_id);
    printf("3. put row (id=42, name=hello)\n");

    /* ── Count ───────────────────────────────────────────────────────── */
    uint64_t count;
    CHECK(mongreldb_table_count(table, &count));
    assert(count == 2);
    printf("4. count = %llu\n", (unsigned long long)count);

    /* ── Query ───────────────────────────────────────────────────────── */
    mongreldb_query_t *query = mongreldb_query_begin();
    mongreldb_result_t *result = mongreldb_table_query(table, query);
    assert(result);

    size_t n = mongreldb_result_count(result);
    assert(n == 2);
    printf("5. query returned %zu rows\n", n);

    /* Read the row (out-parameter pattern) */
    mongreldb_row row;
    CHECK(mongreldb_result_row(result, 0, &row));
    assert(row.row_id == row_id);
    assert(mongreldb_row_cell_count(&row) >= 1);

    mongreldb_cell c0 = {0};
    CHECK(mongreldb_row_cell(&row, 0, &c0));
    printf("6. row 0: col_id=%u, tag=%d, i64=%lld\n", c0.column_id, c0.value.tag, (long long)c0.value.v.i64);

    mongreldb_result_free(result);
    mongreldb_query_free(query);
    CHECK(mongreldb_table_delete(table, row_id));
    CHECK(mongreldb_table_count(table, &count));
    assert(count == 1);
    mongreldb_table_free(table);

    /* ── Transaction ─────────────────────────────────────────────────── */
    mongreldb_transaction_t *txn = mongreldb_begin(db);
    assert(txn);

    for (int64_t i = 100; i < 103; i++) {
        mongreldb_cell_input tcells_buf[2] = {0};
        tcells_buf[0].column_id = 1;
        tcells_buf[0].value.tag = MDB_VALUE_INT64;
        tcells_buf[0].value.v.i64 = i;

        tcells_buf[1].column_id = 2;
        tcells_buf[1].value.tag = MDB_VALUE_BYTES;
        tcells_buf[1].value.v.bytes.data = (const uint8_t *)"row";
        tcells_buf[1].value.v.bytes.len = 3;

        mongreldb_cell_input_array tcells = { .data = tcells_buf, .len = 2 };
        CHECK(mongreldb_txn_put(txn, "items", &tcells));
    }

    uint64_t epoch;
    CHECK(mongreldb_txn_commit(txn, &epoch));
    mongreldb_txn_free(txn);
    printf("7. transaction committed 3 rows (epoch=%llu)\n", (unsigned long long)epoch);

    /* Verify count */
    table = mongreldb_database_table(db, "items");
    CHECK(mongreldb_table_count(table, &count));
    assert(count == 4);
    printf("8. count after txn = %llu\n", (unsigned long long)count);
    mongreldb_table_free(table);

    /* ── Auth ────────────────────────────────────────────────────────── */
    CHECK(mongreldb_create_user(db, "alice", "s3cret"));
    printf("9. user created\n");

    uint8_t ok;
    CHECK(mongreldb_verify_user(db, "alice", "s3cret", &ok));
    assert(ok == 1);
    printf("10. user verified\n");

    CHECK(mongreldb_verify_user(db, "alice", "wrong", &ok));
    assert(ok == 0);
    printf("11. wrong password rejected\n");

    /* ── SQL execution ──────────────────────────────────────────────── */

    /* Create a table via SQL DDL. DDL produces no result rows (empty IPC). */
    uint8_t *sql_buf = NULL;
    size_t sql_len = 0;
    CHECK(mongreldb_database_sql(db,
        "CREATE TABLE sql_items (id INT64 PRIMARY KEY, label VARCHAR, price FLOAT64)",
        &sql_buf, &sql_len));
    assert(sql_len == 0);
    mongreldb_free_sql_result(sql_buf, sql_len);
    printf("12. SQL CREATE TABLE\n");

    /* Insert rows via SQL DML. Also produces no result rows. */
    CHECK(mongreldb_database_sql(db,
        "INSERT INTO sql_items (id, label, price) VALUES (1, 'widget', 9.99)",
        &sql_buf, &sql_len));
    assert(sql_len == 0);
    mongreldb_free_sql_result(sql_buf, sql_len);

    CHECK(mongreldb_database_sql(db,
        "INSERT INTO sql_items (id, label, price) VALUES (2, 'gadget', 19.99)",
        &sql_buf, &sql_len));
    assert(sql_len == 0);
    mongreldb_free_sql_result(sql_buf, sql_len);
    printf("13. SQL INSERT 2 rows\n");

    /* SELECT returns Arrow IPC file bytes (starts with "ARROW1" magic). */
    CHECK(mongreldb_database_sql(db,
        "SELECT id, label, price FROM sql_items ORDER BY id",
        &sql_buf, &sql_len));
    assert(sql_len >= 6);
    assert(memcmp(sql_buf, "ARROW1", 6) == 0);
    mongreldb_free_sql_result(sql_buf, sql_len);
    printf("14. SQL SELECT returned %zu bytes of Arrow IPC\n", sql_len);

    /* SQL error: querying a nonexistent table should fail with a message. */
    int32_t sql_rc = mongreldb_database_sql(db,
        "SELECT * FROM no_such_table", &sql_buf, &sql_len);
    assert(sql_rc < 0);
    assert(strlen(mongreldb_last_error()) > 0);
    mongreldb_free_sql_result(sql_buf, sql_len);
    printf("15. SQL error handled correctly\n");

    /* ── Migration planning ─────────────────────────────────────────── */

    /* Compute checksum for a single create_table migration. */
    const char *ops_json = "[{\"create_table\":{\"name\":\"users\"}}]";
    const char *checksum = NULL;
    CHECK(mongreldb_migration_checksum_json(1, "initial", ops_json, &checksum));
    assert(checksum != NULL);
    assert(strlen(checksum) == 64); /* SHA-256 hex */
    mongreldb_free_migrate_string((char *)checksum);
    printf("16. migration checksum computed\n");

    /* Plan: no applied → all desired are pending. */
    const char *applied = "[]";
    const char *desired =
        "[{\"version\":1,\"name\":\"initial\",\"ops\":[{\"create_table\":{\"name\":\"users\"}}]},"
        "{\"version\":2,\"name\":\"add_idx\",\"ops\":[{\"add_index\":{\"table\":\"users\",\"index\":\"idx\"}}]}]";
    const char *plan_json = NULL;
    CHECK(mongreldb_plan_migrations_json(applied, desired, &plan_json));
    assert(plan_json != NULL);
    assert(strstr(plan_json, "\"version\":1") != NULL);
    assert(strstr(plan_json, "\"version\":2") != NULL);
    mongreldb_free_migrate_string((char *)plan_json);
    printf("17. migration plan: both pending\n");

    /* Plan: version 1 applied → only version 2 pending. */
    applied = "[{\"version\":1,\"name\":\"initial\",\"ops\":[]}]";
    CHECK(mongreldb_plan_migrations_json(applied, desired, &plan_json));
    assert(strstr(plan_json, "\"version\":1") == NULL);
    assert(strstr(plan_json, "\"version\":2") != NULL);
    mongreldb_free_migrate_string((char *)plan_json);
    printf("18. migration plan: only v2 pending\n");

    /* ── Cleanup + RowId reopen identity ────────────────────────────── */
    mongreldb_database_free(db);
    db = mongreldb_open(test_db_dir);
    assert(db);
    table = mongreldb_database_table(db, "items");
    assert(table);
    query = mongreldb_query_begin();
    result = mongreldb_table_query(table, query);
    assert(result);
    int found_second = 0;
    for (size_t i = 0; i < mongreldb_result_count(result); i++) {
        mongreldb_row reopened_row = {0};
        CHECK(mongreldb_result_row(result, i, &reopened_row));
        assert(reopened_row.row_id != row_id);
        found_second |= reopened_row.row_id == second_row_id;
    }
    assert(found_second);
    mongreldb_result_free(result);
    mongreldb_query_free(query);
    mongreldb_table_free(table);
    mongreldb_database_free(db);
    test_authenticated_native_reads();
    printf("19. authenticated native reads enforced\n");
    printf("\nAll C smoke tests passed!\n");
    return 0;
}
