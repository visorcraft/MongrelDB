# Triggers and External Tables

MongrelDB implements triggers and virtual tables through cataloged, validated
engine structures. SQL is one frontend over the same commit and module paths
used by Rust, HTTP, NAPI, and Kit clients.

## Triggers

Triggers are row-level and support `INSERT`, `UPDATE`, and `DELETE` events.
Ordinary tables support `BEFORE` and `AFTER` triggers. Session views support
`INSTEAD OF` triggers.

```sql
CREATE TRIGGER audit_order
AFTER UPDATE OF status ON orders
WHEN OLD.status <> NEW.status
BEGIN
  INSERT INTO order_audit(order_id, status)
  VALUES (NEW.id, NEW.status);
END;

DROP TRIGGER audit_order;
```

Trigger programs can reference `OLD.column` and `NEW.column`. Catalog/API
callers can also use the validated trigger IR directly, including deterministic
`BEFORE` row replacement.

### Commit behavior

- Trigger expansion happens in `Database::commit_transaction`.
- Original writes and trigger-produced writes commit in one WAL transaction.
- Unique, foreign-key, CHECK, authorization, and row-policy validation apply to
  the final expanded write set.
- A trigger error leaves no partial durable write.
- Stable catalog creation order determines trigger firing order. Replacing a
  trigger preserves its position.
- Dropping or renaming tables, views, and columns updates trigger dependencies.

### Recursion

Recursive triggers are disabled by default. Enable them per SQL session:

```sql
PRAGMA recursive_triggers = 1;
```

Cycles and maximum-depth violations return errors with the trigger stack.
Applications using the native API can configure recursion and depth through
`TriggerConfig`.

### RAISE

Trigger bodies support `RAISE(ABORT, message)`, `RAISE(FAIL, message)`,
`RAISE(ROLLBACK, message)`, and `RAISE(IGNORE)` through `SELECT RAISE(...)`.
MongrelDB keeps statement effects atomic, so `FAIL` and `ROLLBACK` do not retain
SQLite-style partial row effects. `IGNORE` suppresses the current row operation
and later triggers for that row while preserving trigger work already staged.

### Introspection and APIs

```sql
PRAGMA trigger_list;
```

Rust exposes `create_trigger`, `drop_trigger`, `triggers`, and trigger
configuration on `Database`. The daemon, Rust HTTP client, and local or remote
NAPI bindings expose the same catalog operations. Trigger DDL supports
idempotency keys on remote paths. Keys are durably bound to the authenticated
owner, operation, and complete trigger definition or dropped trigger name.
Retries replay the exact response; mismatched input or an uncertain prior
commit fails closed without executing trigger DDL again.
The daemon checks current DDL permission before receipt lookup and binds the
security version plus referenced table and schema IDs. Publication also fences
the catalog epoch and existing trigger revision; replay verifies the exact
stored trigger. Permission changes, trigger replacement, or table recreation
cannot replay an old trigger response.

## External Table Modules

External modules back `CREATE VIRTUAL TABLE` and eponymous table-valued
functions without loading arbitrary native code from SQL.

```sql
CREATE VIRTUAL TABLE numbers USING series(1, 100, 1);
SELECT value FROM numbers WHERE value >= 95;
```

`ExternalTableModule` describes schema, capabilities, planning, reads, writes,
and optional indexes. `MongrelSession` owns the module registry. Applications
with cataloged custom tables must register their module implementations again
when reopening a session.

The planner passes projections, filters, ordering, limits, and estimates to the
module. Modules report accepted filters and residual-filter requirements.
MongrelDB applies remaining filters and exact limits.

### Built-in modules

| Module | Behavior |
|---|---|
| `series` | Read-only integer series with filter and limit pushdown |
| `json_each`, `json_tree` | Read-only JSON traversal |
| `jsonb_each`, `jsonb_tree` | Read-only JSONB traversal |
| `schema_tables` | Live schema metadata |
| `dbstat` | Live database statistics |
| `kv_store` | Durable writable key/value table |
| `fts_docs` | Writable full-text table with ranking, snippets, and highlights |
| `rtree_rects` | Writable rectangle table with intersection filtering |

`fts_docs` accepts tokenizer, prefix-query, case-sensitivity, minimum-token,
and stopword options. It supports `AND`, `OR`, `NOT`, phrases, prefixes, and
common `MATCH` forms. `rtree_rects` uses `rtree_intersects(...)` for exact
rectangle overlap.

### Writes and durability

Writable modules use `ExternalTxn` and `ExternalWriteOp`. Module state is
staged with ordinary table writes and committed through the shared WAL.
Recovery restores committed state under `_vtab/` before providers reconnect.
Backup copies `_vtab/`; `check()` reports orphan state and `gc()` removes it.

Explicit SQL transactions may mix base-table and writable-module changes. A
module must declare writable and transaction-safe capabilities before trigger
programs can target it. Trigger-produced external writes pass through the
`ExternalTriggerBridge` and remain atomic with the base writes.

### App-provided modules

Register custom modules directly on `MongrelSession`, or pass them to
`open_with_external_modules(...)` when reopening cataloged virtual tables. A
daemon embedding can provide an allowlist with
`build_app_with_external_modules(...)`. SQL dynamic library loading remains
disabled.

### Introspection

External tables and modules appear through:

```sql
PRAGMA table_list;
PRAGMA table_xinfo('table_name');
PRAGMA module_list;
```

Query traces identify module scans as `ScanMode::ExternalModule`.
