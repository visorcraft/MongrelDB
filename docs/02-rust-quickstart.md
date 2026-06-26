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

- `PRIMARY_KEY` — marks the column as the primary key (enables fast point lookups)
- `NULLABLE` — the column can contain null values
- `ENCRYPTED_INDEXABLE` — the column is encrypted but still searchable (requires encryption)

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
version — MongrelDB keeps track of which version is current.

### Batch Insert

For inserting many rows at once, use `put_batch` — it groups them into one
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
let count = db.count();  // O(1) — doesn't scan any data
```

### Delete a Row

```rust
db.delete(row_id).unwrap();
db.commit().unwrap();
```

## Querying with Conditions

The Condition API lets you search using indexes without SQL. You build a
query by combining conditions — all conditions are ANDed together.

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
| `Range { column_id, lo, hi }` | Integer column in a range | PGM learned index |
| `RangeF64 { column_id, lo, hi, ... }` | Float column in a range | PGM / page prune |
| `FmContains { column_id, pattern }` | Text contains a substring | FM-index |
| `Ann { column_id, query, k }` | k nearest neighbors by vector | HNSW |
| `SparseMatch { column_id, query, k }` | Top-k sparse vector match | Inverted index |

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

Compaction is safe to run while reads are happening — MVCC snapshots ensure
readers see a consistent view.

## Closing

MongrelDB saves to disk on every `commit()` and `flush()`. When your program
exits, any uncommitted data in the WAL will be replayed on next open. You can
also explicitly flush and close:

```rust
db.flush().unwrap();  // commit + move data to columnar format
// db is dropped here — Rust's Drop trait handles cleanup
```

## Reopening

```rust
let db = Db::open("./mydb").unwrap();
```

This reads the manifest, replays the WAL, and rebuilds indexes. If a
global index checkpoint exists, it's loaded directly (fast reopen). Otherwise
indexes are rebuilt from the sorted runs.
