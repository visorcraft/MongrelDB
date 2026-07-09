# Rust Quick Start

This guide covers the full Rust API for reading, writing, querying, and
managing MongrelDB tables.

## Creating a Table

Every table needs a schema. A schema defines your columns (name, type, flags)
and which indexes to build.

```rust
use mongreldb_core::schema::*;
use mongreldb_core::Db;

let schema = Schema {
    schema_id: 1,
    columns: vec![
        ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        },
        ColumnDef {
            id: 2,
            name: "email".into(),
            ty: TypeId::Bytes,
            flags: ColumnFlags::empty(),
        },
        ColumnDef {
            id: 3,
            name: "score".into(),
            ty: TypeId::Float64,
            flags: ColumnFlags::empty(),
        },
    ],
    indexes: vec![
        // Add a bitmap index on email for fast equality lookups
        IndexDef { name: "email_bm".into(), column_id: 2, kind: IndexKind::Bitmap },
    ],
    colocation: vec![],
};

let mut db = Db::create("./mydb", schema, 1).unwrap();
```

### Column Types

| Type | Rust type | Use for |
|---|---|---|
| `Int64` | `i64` | Integers, timestamps, IDs |
| `Float64` | `f64` | Decimals, measurements |
| `Bytes` | `Vec<u8>` | Text (UTF-8), binary data |
| `Bool` | `bool` | True/false |

### Column Flags

- `PRIMARY_KEY` - marks the column as the primary key (enables fast point lookups)
- `NULLABLE` - the column can contain null values
- `ENCRYPTED_INDEXABLE` - the column is encrypted but still searchable (requires encryption)

## Writing Data

### Single Row

```rust
use mongreldb_core::Value;

let row_id = db.put(vec![
    (1, Value::Int64(42)),
    (2, Value::Bytes(b"alice@example.com".to_vec())),
    (3, Value::Float64(98.5)),
]).unwrap();

db.commit().unwrap();  // make it durable
```

`put()` always creates a new row. To "update" an existing row, you write a new
version - MongrelDB keeps track of which version is current.

### Batch Insert

For inserting many rows at once, use `put_batch` - it groups them into one
WAL write:

```rust
let batch: Vec<Vec<(u16, Value)>> = (0..1000)
    .map(|i| vec![
        (1, Value::Int64(i)),
        (2, Value::Bytes(format!("user{i}@test.com").into_bytes())),
        (3, Value::Float64(i as f64 * 1.5)),
    ])
    .collect();

let row_ids = db.put_batch(batch).unwrap();
db.commit().unwrap();
```

### Bulk Load (Fastest Ingest)

For loading large datasets (thousands to millions of rows), use `bulk_load`.
It bypasses the WAL entirely and writes directly to columnar storage:

```rust
let rows: Vec<Vec<(u16, Value)>> = (0..1_000_000)
    .map(|i| vec![
        (1, Value::Int64(i)),
        (2, Value::Bytes(format!("user{i}").into_bytes())),
        (3, Value::Float64(i as f64)),
    ])
    .collect();

db.bulk_load(rows).unwrap();  // ~26M rows/sec with typed columns
```

For even more speed, use typed columns directly (no `Value` enum overhead):

```rust
use mongreldb_core::columnar::NativeColumn;

db.bulk_load_columns(vec![
    (1, NativeColumn::Int64 { data: (0..1_000_000).collect(), validity: vec![0xFF; 125_000] }),
    // ... other columns
]).unwrap();
```

## Reading Data

### Get a Single Row

```rust
let snap = db.snapshot();
let row = db.get(row_id, snap).unwrap();
let name = row.columns.get(&2);
```

### Get the Row Count

```rust
let count = db.count();  // O(1) - doesn't scan any data
```

### Delete a Row

```rust
db.delete(row_id).unwrap();
db.commit().unwrap();
```

## Querying with Conditions

The Condition API lets you search using indexes without SQL. You build a
query by combining conditions - all conditions are ANDed together.

```rust
use mongreldb_core::query::{Condition, Query};

// Find rows where email = "alice@example.com"
let q = Query::new().and(Condition::BitmapEq {
    column_id: 2,
    value: b"alice@example.com".to_vec(),
});

let results = db.query(&q).unwrap();
println!("Found {} rows", results.len());
```

### Available Conditions

| Condition | What it does | Index used |
|---|---|---|
| `Pk(key)` | Exact primary-key lookup | HOT (trie) |
| `BitmapEq { column_id, value }` | Column equals a value | Roaring bitmap |
| `BitmapIn { column_id, values }` | Column is one of several values | Bitmap union |
| `BytesPrefix { column_id, prefix }` | Anchored `LIKE 'prefix%'` on a Bytes column | Bitmap key-prefix scan (exact) |
| `Range { column_id, lo, hi }` | Integer column in a range | PGM learned index |
| `RangeF64 { column_id, lo, hi, ... }` | Float column in a range | PGM / page prune |
| `FmContains { column_id, pattern }` | Text contains a substring | FM-index |
| `FmContainsAll { column_id, patterns }` | Text contains all substrings | FM-index intersection |
| `Ann { column_id, query, k }` | k nearest neighbors by vector | HNSW |
| `SparseMatch { column_id, query, k }` | Top-k sparse vector match | Inverted index |
| `MinHashSimilar { column_id, query, k }` | Top-k Jaccard set similarity | MinHash LSH |
| `IsNull { column_id }` / `IsNotNull { column_id }` | Null check | Page-stat prune |

### Combining Conditions

Stack them to intersect results. This finds rows that match ALL conditions:

```rust
let q = Query::new()
    .and(Condition::BitmapEq { column_id: 2, value: b"premium".to_vec() })
    .and(Condition::RangeF64 {
        column_id: 3, lo: 90.0, lo_inclusive: true, hi: 100.0, hi_inclusive: true,
    });

let results = db.query(&q).unwrap();
```

See [Native Queries](05-native-queries.md) for a deep dive on condition
combinations.

## SQL Queries (CTEs, Windows, Regex, Catalog)

For analytical queries, joins, and SQL-stdlib features, use `MongrelSession`
which wraps DataFusion 54. This gives you the full SQL surface - recursive CTEs,
window functions, `regexp()`, catalog introspection, and more:

```rust
use mongreldb_query::MongrelSession;
use std::sync::Arc;

let session = MongrelSession::open(Arc::new(db))?;

// Recursive CTE - tree traversal.
let batches = session.run("
    WITH RECURSIVE tree AS (
        SELECT id, parent, 0 AS depth FROM nodes WHERE parent IS NULL
        UNION ALL
        SELECT n.id, n.parent, t.depth + 1
        FROM nodes n JOIN tree t ON n.parent = t.id
    )
    SELECT id, depth FROM tree ORDER BY id
").await?;

// Window function - ranking within partitions.
let batches = session.run("
    SELECT category, amount,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rank
    FROM orders
").await?;

// Regex matching.
let batches = session.run("
    SELECT id FROM users WHERE regexp('^admin.*', name) = 1
").await?;

// Catalog introspection.
let batches = session.run("SELECT type, name FROM information_schema.tables ORDER BY name").await?;

// Cross-database query via ATTACH.
session.run("ATTACH '/path/to/other' AS other").await?;
let batches = session.run("SELECT id FROM other_users WHERE id > 100").await?;
session.run("DETACH other").await?;

// Sub-transactions (SAVEPOINT).
session.run("BEGIN").await?;
session.run("INSERT INTO logs VALUES (1, 'hello')").await?;
session.run("SAVEPOINT sp1").await?;
session.run("INSERT INTO logs VALUES (2, 'world')").await?;
session.run("ROLLBACK TO sp1").await?;  // discards 'world', keeps 'hello'
session.run("COMMIT").await?;
```

See [SQL Queries](04-sql-queries.md) for the full SQL surface and
[Operational SQL Commands](12-operational-sql-commands.md) for catalog/maintenance
commands.

## Transactions

MongrelDB uses a simple commit model. All `put()` and `delete()` calls go into
a pending state. `commit()` makes them visible to new snapshots and durable
on disk.

```rust
db.put(row_a).unwrap();
db.put(row_b).unwrap();
db.delete(old_row).unwrap();
db.commit().unwrap();  // all three become visible atomically
```

There are no explicit "begin transaction" calls. Everything between two
`commit()` calls is one transaction.

## Compaction

Over time, updates and deletes create old data that wastes space. Compaction
merges sorted runs and removes obsolete data:

```rust
db.compact().unwrap();  // merges all runs into one, removes dead rows
```

Compaction is safe to run while reads are happening - MVCC snapshots ensure
readers see a consistent view.

## Users, Roles & Permissions

Catalog users have Argon2id-hashed passwords and belong to zero or more
roles; each role carries a set of permissions. See
**[Users, Roles & Permissions](14-auth.md)** for the full model.

```rust
use mongreldb_core::Database;
use mongreldb_core::auth::Permission;

let db = Database::open("./mydb")?;

// Users
db.create_user("alice", "s3cret-pw")?;
db.alter_user_password("alice", "new-pw")?;
assert!(db.verify_user("alice", "new-pw")?.is_some());
db.set_user_admin("alice", true)?;
for u in db.users() { println!("{}", u.username); }

// Roles + permissions
db.create_role("analyst")?;
db.grant_permission("analyst", Permission::Select { table: "orders".into() })?;
db.grant_permission("analyst", Permission::Insert { table: "orders".into() })?;
db.grant_role("alice", "analyst")?;

// Application-layer access check
assert!(db.check_permission("alice", &Permission::Select { table: "orders".into() }));
```

### Credential enforcement (require_auth)

By default, permissions are advisory. To make the storage layer enforce them
on every operation, create or convert the database with `require_auth`:

```rust
use mongreldb_core::Database;
use mongreldb_core::auth::Permission;

// Create a database that requires credentials for every operation.
let db = Database::create_with_credentials("./mydb", "admin", "s3cret-pw")?;

// Reopen requires credentials.
let db = Database::open_with_credentials("./mydb", "admin", "s3cret-pw")?;

// Convert an existing credentialless database.
let db = Database::open("./existing")?;
db.enable_auth("admin", "s3cret-pw")?;

// Revert to credentialless (recovery).
let db = Database::open_with_credentials("./mydb", "admin", "s3cret-pw")?;
db.disable_auth()?;

// Encrypted + credentialed.
let db = Database::create_encrypted_with_credentials(
    "./secure", "passphrase", "admin", "s3cret-pw",
)?;
```

See **[Credential Enforcement](15-credential-enforcement.md)** for the full
matrix and per-operation permission requirements.

For the HTTP daemon, start with `--auth-token <token>` (Bearer),
`--auth-users` (HTTP Basic against catalog users), or both. See
**[Daemon Mode](08-daemon.md#authentication)**.

## Closing

MongrelDB saves to disk on every `commit()` and `flush()`. When your program
exits, any uncommitted data in the WAL will be replayed on next open. You can
also explicitly flush and close:

```rust
db.flush().unwrap();  // commit + move data to columnar format
// db is dropped here - Rust's Drop trait handles cleanup
```

## Reopening

```rust
let db = Db::open("./mydb").unwrap();
```

This reads the manifest, replays the WAL, and rebuilds indexes. If a
global index checkpoint exists, it's loaded directly (fast reopen). Otherwise
indexes are rebuilt from the sorted runs.

## Cross-Process Lock Contention

By default, [`Database::open`] is **fail-fast**: if another process already
holds the cross-process lock on `<dir>/_meta/.lock`, the open returns
`MongrelError::Io` immediately. That mirrors the historical `try_lock_exclusive`
semantics and lets coordinating callers bring their own retry logic.

If you'd rather have SQLite-style `busy_timeout` semantics - block for up to
N ms waiting for the lock before giving up - opt in with
[`Database::open_with_options`].

```rust
use mongreldb_core::{Database, OpenOptions};

let opts = OpenOptions::default().with_lock_timeout_ms(5_000);
let db = Database::open_with_options("./mydb", opts)?;
```

Backoff schedule: 1ms → 10ms → 50ms, capped at `lock_timeout_ms`. Set `0` to
keep the fail-fast default.
