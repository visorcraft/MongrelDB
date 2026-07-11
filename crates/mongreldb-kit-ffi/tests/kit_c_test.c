/*
 * C smoke test for the mongreldb-kit-ffi C ABI.
 *
 * Compiled by tests/kit_c_smoke_test.rs and linked against libmongreldb_kit.
 * Exercises: create → SQL → migrate → query builder → cleanup.
 */
#include "mongreldb_kit.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>

#define CHECK(call) do { \
    int32_t _rc = (call); \
    if (_rc != 0) { \
        fprintf(stderr, "FAIL: %s returned %d: %s\n", #call, _rc, mongreldb_kit_last_error()); \
        exit(1); \
    } \
} while(0)

static const char *SCHEMA_JSON =
    "{\"tables\":[{\"id\":1,\"name\":\"users\","
    "\"columns\":["
    "{\"id\":1,\"name\":\"id\",\"storage_type\":\"int64\",\"application_type\":\"int64\",\"nullable\":false,\"primary_key\":true,\"default\":null,\"generated\":false},"
    "{\"id\":2,\"name\":\"name\",\"storage_type\":\"text\",\"application_type\":\"text\",\"nullable\":true,\"primary_key\":false,\"default\":null,\"generated\":false}"
    "],\"primary_key\":[\"id\"]}]}";

static char *make_tmpdir(void) {
    char *s = malloc(256);
    snprintf(s, 256, "/tmp/mdb_kit_c_%d_%ld", (int)getpid(), (long)time(NULL));
    return s;
}

int main(void) {
    char *path = make_tmpdir();

    /* ── Create Kit database ────────────────────────────────────────── */
    mongreldb_kit_database_t *db = mongreldb_kit_create(path, SCHEMA_JSON);
    if (!db) {
        fprintf(stderr, "create failed: %s\n", mongreldb_kit_last_error());
        return 1;
    }
    printf("1. kit database created\n");

    /* ── SQL: insert + select ───────────────────────────────────────── */
    const char *json = NULL;
    CHECK(mongreldb_kit_sql_rows(db,
        "INSERT INTO users (id, name) VALUES (1, 'alice')", &json));
    mongreldb_kit_free_json((char *)json);
    printf("2. SQL insert\n");

    CHECK(mongreldb_kit_sql_rows(db, "SELECT id, name FROM users", &json));
    assert(strstr(json, "alice") != NULL);
    mongreldb_kit_free_json((char *)json);
    printf("3. SQL select (JSON rows)\n");

    /* ── SQL Arrow IPC ──────────────────────────────────────────────── */
    uint8_t *arrow = NULL;
    size_t arrow_len = 0;
    CHECK(mongreldb_kit_sql_arrow(db, "SELECT id FROM users", &arrow, &arrow_len));
    assert(arrow_len >= 6);
    assert(memcmp(arrow, "ARROW1", 6) == 0);
    mongreldb_kit_free_arrow(arrow, arrow_len);
    printf("4. SQL select (Arrow IPC: %zu bytes)\n", arrow_len);

    /* ── Migration ──────────────────────────────────────────────────── */
    const char *migrations =
        "[{\"version\":1,\"name\":\"add_orders\","
        "\"ops\":[{\"raw_sql\":\"CREATE TABLE orders (id INT64 PRIMARY KEY, total FLOAT64)\"}]}]";
    CHECK(mongreldb_kit_migrate_json(db, migrations));
    printf("5. migration applied\n");

    /* Verify migration recorded. */
    CHECK(mongreldb_kit_applied_migrations_json(db, &json));
    assert(strstr(json, "add_orders") != NULL);
    mongreldb_kit_free_json((char *)json);
    printf("6. applied migrations read back\n");

    /* ── Query builder: SELECT ──────────────────────────────────────── */
    const char *select_q =
        "{\"table\":\"users\",\"columns\":[],\"filter\":null,\"order_by\":[],\"limit\":null,\"offset\":null}";
    CHECK(mongreldb_kit_query_select_json(db, select_q, &json));
    assert(strstr(json, "alice") != NULL);
    mongreldb_kit_free_json((char *)json);
    printf("7. query builder SELECT\n");

    /* ── Cleanup ────────────────────────────────────────────────────── */
    mongreldb_kit_database_free(db);
    free(path);
    printf("\nAll Kit C smoke tests passed!\n");
    return 0;
}
