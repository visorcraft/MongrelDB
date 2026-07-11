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

static mongreldb_database_t *create_test_db(void) {
    char tmpl[] = "/tmp/mdb_c_test_XXXXXX";
    char *dir = mkdtemp(tmpl);
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

int main(void) {
    /* ── Database lifecycle ──────────────────────────────────────────── */
    mongreldb_database_t *db = create_test_db();
    printf("1. database created\n");

    /* ── Schema + table ──────────────────────────────────────────────── */
    make_simple_table(db, "items");
    printf("2. table created\n");

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
    printf("3. put row (id=42, name=hello)\n");

    /* ── Count ───────────────────────────────────────────────────────── */
    uint64_t count;
    CHECK(mongreldb_table_count(table, &count));
    assert(count == 1);
    printf("4. count = %llu\n", (unsigned long long)count);

    /* ── Query ───────────────────────────────────────────────────────── */
    mongreldb_query_t *query = mongreldb_query_begin();
    mongreldb_result_t *result = mongreldb_table_query(table, query);
    assert(result);

    size_t n = mongreldb_result_count(result);
    assert(n == 1);
    printf("5. query returned %zu rows\n", n);

    /* Read the row (out-parameter pattern) */
    mongreldb_row row;
    CHECK(mongreldb_result_row(result, 0, &row));
    assert(mongreldb_row_cell_count(&row) >= 1);

    mongreldb_cell c0 = mongreldb_row_cell(&row, 0);
    printf("6. row 0: col_id=%u, tag=%d, i64=%lld\n", c0.column_id, c0.value.tag, (long long)c0.value.v.i64);

    mongreldb_result_free(result);
    mongreldb_query_free(query);
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

    /* ── Cleanup ─────────────────────────────────────────────────────── */
    mongreldb_database_free(db);
    printf("\nAll C smoke tests passed!\n");
    return 0;
}
