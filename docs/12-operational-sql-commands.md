# Operational SQL Commands

MongrelDB accepts a small operational SQL layer for tooling, migrations, and
debugging. These commands expose schema metadata, function metadata, integrity
checks, maintenance hooks, and planner inspection through SQL result batches.

## PRAGMA Introspection

Read-only PRAGMAs return compatibility-shaped Arrow batches:

| Command | Purpose |
|---|---|
| `PRAGMA table_info(table_name)` | Column id, name, type, nullable flag, default value, and primary-key ordinal |
| `PRAGMA table_xinfo(table_name)` | `table_info` plus a `hidden` column |
| `PRAGMA table_list` | Tables and views in the main schema |
| `PRAGMA index_list(table_name)` | Declared indexes for a table |
| `PRAGMA index_info(index_name)` | Indexed columns for one declared index |
| `PRAGMA index_xinfo(index_name)` | Indexed columns plus sort, collation, and key flags |
| `PRAGMA foreign_key_list(table_name)` | Engine-side foreign-key constraints |
| `PRAGMA foreign_key_check` | Foreign-key violations as `table`, `rowid`, `parent`, `fkid`; empty when clean |
| `PRAGMA database_list` | Main database name and storage path |
| `PRAGMA function_list` | MongrelDB-registered SQL functions, aggregate aliases, and table functions |
| `PRAGMA module_list` | Registered table functions |
| `PRAGMA trigger_list` | Cataloged trigger name, target, timing, event, enabled flag, and epochs |
| `PRAGMA collation_list` | Available collations |
| `PRAGMA compile_options` | Build/capability flags exposed to SQL tooling |
| `PRAGMA integrity_check` | Full database check result, returning `ok` when clean |
| `PRAGMA quick_check` | Same result shape as `integrity_check` |
| `PRAGMA schema_version` | Deterministic schema fingerprint |
| `PRAGMA user_version` | Metadata-backed application version integer |
| `PRAGMA application_id` | Metadata-backed application id integer |
| `PRAGMA data_version` | Current visible database epoch |
| `PRAGMA foreign_keys` | Always `1`; declarative constraints are enforced by the engine |
| `PRAGMA query_only` | Always `0` |
| `PRAGMA journal_mode` | Returns `wal` |
| `PRAGMA synchronous` | Returns `1` |
| `PRAGMA encoding` | Returns `UTF-8` |
| `PRAGMA page_size` | Returns the 4096-byte accounting unit used by `page_count` |
| `PRAGMA page_count` | Approximate database directory size divided by `page_size` |
| `PRAGMA freelist_count` | Returns `0`; MongrelDB reclaims obsolete files through GC |
| `PRAGMA cache_size` | Returns a compatibility default |
| `PRAGMA automatic_index` | Returns `1`; index build policy remains an engine API |
| `PRAGMA defer_foreign_keys` | Returns `0`; constraints are validated at commit |
| `PRAGMA recursive_triggers` | Returns or sets recursive trigger firing (`0` by default) |
| `PRAGMA trusted_schema` | Returns `0` |
| `PRAGMA wal_checkpoint` | Flushes live tables, runs GC, and returns `busy`, `log`, `checkpointed` |
| `PRAGMA optimize` | Ensures indexes are complete and clears stale SQL caches |

`user_version` and `application_id` can be assigned with `PRAGMA name = integer`
and are persisted in the database metadata directory. Storage durability,
constraint enforcement, WAL behavior, and index policy remain under engine APIs
rather than mutable SQL session flags.

Unknown PRAGMAs are accepted and return an empty result, matching SQLite's
historical "ignore unknown PRAGMA" behavior for tooling compatibility.

## Maintenance Commands

```sql
ANALYZE;
REINDEX;
REINDEX idx_name;
VACUUM;
VACUUM INTO '/path/to/backup-dir';
```

`ANALYZE` ensures deferred indexes are complete and clears the SQL caches.
MongrelDB already keeps live table counts and page statistics in engine
metadata, so this does not build a separate statistics catalog.

`REINDEX` without a target ensures indexes are complete, compacts every table,
and runs garbage collection. `REINDEX table_name` compacts that table.
`REINDEX index_name` finds the owning table and compacts it.

`VACUUM` compacts all tables, runs garbage collection, and clears SQL caches.
It is equivalent to the existing maintenance path exposed through Rust, Node,
HTTP, and the CLI.

`VACUUM INTO` first performs the same compaction and GC, then copies the database
directory to a new target directory. The target must not already exist and must
not be inside the source database directory.

`ATTACH 'path' AS alias` opens a second MongrelDB database directory and
registers all its tables on the current session's DataFusion context. Tables are
available under the qualified name `alias_<table>` (underscore-qualified, since
DataFusion's `schema.table` resolution requires catalog setup). `DETACH alias`
removes the attached tables. This enables cross-database SQL queries within one
session (e.g. `SELECT * FROM other_users JOIN local_orders`).

`SAVEPOINT name` / `RELEASE name` / `ROLLBACK TO name` provide nested
sub-transaction control within a SQL `BEGIN`/`COMMIT` block. Savepoints mark a
position in the staged-ops vector; `ROLLBACK TO` discards ops back to that
position without aborting the outer transaction.

`SELECT * FROM sqlite_master` (or `sqlite_schema`) returns a SQLite-compatible
catalog listing all tables, views, and triggers in the session. Columns:
`type`, `name`, `tbl_name`, `rootpage`, `sql`.

`regexp('pattern', value)` is a scalar UDF returning 1 (match) or 0 (no match),
using the `regex` crate. Invalid patterns return 0 (SQLite semantics).

## Trigger Commands

`CREATE TRIGGER` and `DROP TRIGGER` store row-level trigger programs in the
MongrelDB catalog. Ordinary tables support `BEFORE` and `AFTER` triggers for
`INSERT`, `UPDATE`, and `DELETE`; session views with explicit column aliases
support `INSTEAD OF INSERT`, `INSTEAD OF UPDATE`, and `INSTEAD OF DELETE`
routing through trigger IR for simple one-table projection views. More complex
view queries remain reserved until the SQL layer has a broader OLD-row
materialization path. `PRAGMA trigger_list` exposes cataloged triggers in firing
order for migration and debugging tools.

## Planner Inspection

```sql
EXPLAIN QUERY PLAN
SELECT * FROM events WHERE tenant_id = 42;

EXPLAIN
SELECT * FROM events WHERE tenant_id = 42;
```

`EXPLAIN QUERY PLAN` returns four columns: `id`, `parent`, `notused`, and
`detail`. The first rows are MongrelDB high-level planner summaries such as
`SCAN table`, `SEARCH table USING MONGREL INDEX`, `USE TEMP B-TREE FOR GROUP
BY`, and `USE TEMP B-TREE FOR ORDER BY`; the remaining rows include DataFusion
logical and physical plan details.

Bare `EXPLAIN` is delegated to DataFusion and returns its `plan_type`/`plan`
diagnostic table. MongrelDB does not expose SQLite virtual-machine bytecode,
because it does not execute SQL through SQLite's VM.

## Full Profile Status

Implemented beyond the MVP: broader PRAGMA introspection, metadata-backed
`user_version`/`application_id`, storage accounting PRAGMAs, `wal_checkpoint`,
`foreign_key_check`, `VACUUM INTO`, aggregate-aware `function_list`,
`ATTACH`/`DETACH` (cross-database queries), `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`
(session-level sub-transactions), `sqlite_master`/`sqlite_schema` (catalog
introspection), `regexp()` (regex matching UDF), recursive CTEs
(`WITH RECURSIVE`), window functions (`OVER`/`PARTITION BY`), and
`EXPLAIN`/`EXPLAIN QUERY PLAN`.
