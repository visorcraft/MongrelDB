# Migrations via the FFI

The C ABI exposes migration **planning** and **checksums** (language-neutral
logic shared with all bindings). Migration **execution** is orchestrated by the
host language, translating each `MigrationOp` into the appropriate FFI call.

## Planning

Use `mongreldb_plan_migrations_json` to determine which migrations are pending:

```c
const char *applied = "[...]";  // migrations already recorded in the db
const char *desired = "[...]";  // full app-defined ordered set
const char *pending_json = NULL;

if (mongreldb_plan_migrations_json(applied, desired, &pending_json) == MDB_OK) {
    // pending_json is a JSON array of pending Migration objects, sorted by version.
    mongreldb_free_migrate_string((char *)pending_json);
}
```

## Checksums

Use `mongreldb_migration_checksum_json` to compute the canonical SHA-256 of a
migration. This is byte-for-byte identical across all language bindings (Rust,
TypeScript, Python, C, C++, etc.):

```c
const char *ops_json = "[{\"create_table\":{\"name\":\"users\"}}]";
const char *checksum = NULL;

mongreldb_migration_checksum_json(1, "initial", ops_json, &checksum);
// checksum is a 64-char hex string, e.g. "a1b2c3..."
mongreldb_free_migrate_string((char *)checksum);
```

## Execution: MigrationOp to FFI mapping

Each `MigrationOp` variant maps to one or more FFI calls. The host language
iterates the pending migrations and applies each op:

| `MigrationOp` variant | FFI calls |
|---|---|
| `create_table` | `mongreldb_schema_begin` + `mongreldb_schema_add_column` (per column) + `mongreldb_schema_add_index` / `mongreldb_schema_add_unique` / `mongreldb_schema_add_foreign_key` (as needed) + `mongreldb_schema_build` + `mongreldb_create_table` |
| `drop_table` | `mongreldb_drop_table` |
| `add_column` | `mongreldb_database_sql` with `ALTER TABLE ... ADD COLUMN ...` |
| `drop_column` | `mongreldb_database_sql` with `ALTER TABLE ... DROP COLUMN ...` |
| `alter_column` | `mongreldb_database_sql` with `ALTER TABLE ... ALTER COLUMN ...` |
| `add_index` | `mongreldb_database_sql` with `CREATE INDEX ...` |
| `drop_index` | `mongreldb_database_sql` with `DROP INDEX ...` |
| `add_unique` | `mongreldb_database_sql` with DDL |
| `drop_unique` | `mongreldb_database_sql` with DDL |
| `add_foreign_key` | `mongreldb_database_sql` with DDL |
| `drop_foreign_key` | `mongreldb_database_sql` with DDL |
| `add_check` | `mongreldb_database_sql` with DDL |
| `drop_check` | `mongreldb_database_sql` with DDL |
| `create_procedure` | `mongreldb_database_sql` with `CREATE PROCEDURE ...` |
| `replace_procedure` | `mongreldb_database_sql` with `CREATE OR REPLACE PROCEDURE ...` |
| `drop_procedure` | `mongreldb_database_sql` with `DROP PROCEDURE ...` |
| `create_trigger` | `mongreldb_database_sql` with `CREATE TRIGGER ...` |
| `replace_trigger` | `mongreldb_database_sql` with `CREATE OR REPLACE TRIGGER ...` |
| `drop_trigger` | `mongreldb_database_sql` with `DROP TRIGGER ...` |
| `create_virtual_table` | `mongreldb_database_sql` with `CREATE VIRTUAL TABLE ...` |
| `drop_virtual_table` | `mongreldb_database_sql` with `DROP TABLE ...` |
| `create_view` | `mongreldb_database_sql` with `CREATE VIEW ...` |
| `replace_view` | `mongreldb_database_sql` with `CREATE OR REPLACE VIEW ...` |
| `drop_view` | `mongreldb_database_sql` with `DROP VIEW ...` |
| `raw_sql` | `mongreldb_database_sql` with the raw SQL string |

After applying a migration that creates or drops tables via the FFI (not via
SQL), call `mongreldb_database_sql_refresh` so the cached SQL session sees the
updated table set.

## Tracking applied migrations

The host language is responsible for recording which migrations have been
applied. A common pattern is a `__migrations` table:

```sql
CREATE TABLE __migrations (version INT64 PRIMARY KEY, name VARCHAR, checksum VARCHAR, applied_at TIMESTAMP)
```

After each migration's ops are applied, insert a row and commit. On startup,
read the table to build the `applied` list for `mongreldb_plan_migrations_json`.
