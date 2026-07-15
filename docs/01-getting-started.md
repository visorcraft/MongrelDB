# Getting Started

## What is MongrelDB?

MongrelDB is an embedded database - meaning it runs inside your application
process, not as a separate server. Think of it like SQLite: no separate
database server to install or manage. You add it to your project, open a
database directory, and start reading and writing data.

MongrelDB combines operational writes with six public secondary index kinds,
including text, vector, sparse, set-similarity, equality, and range search.

## Prerequisites

You need one of these:

- **Current stable Rust** to use MongrelDB as a Rust library
- **Node.js 22 or newer** to use MongrelDB as a Node.js addon
- Both is fine too

### Installing Rust

If you don't have Rust yet:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
rustc --version
```

### Installing Node.js

Download from [nodejs.org](https://nodejs.org) or use a version manager like
`nvm`. Verify with:

```sh
node --version  # should print v22 or higher
```

## Installation

### Option A: Use MongrelDB in a Rust project

Add MongrelDB to your `Cargo.toml` (both crates are published to crates.io):

```toml
[dependencies]
mongreldb-core = "0.55.0"   # storage engine
mongreldb-query = "0.55.0"  # SQL frontend
```

For encryption support, add the `encryption` feature:

```toml
mongreldb-core = { version = "0.55.0", features = ["encryption"] }
```

To build against a local checkout of the engine instead, use path dependencies
or a `[patch.crates-io]` block pointing at the repo.

### Option B: Use MongrelDB in a Node.js project

```sh
npm install @visorcraft/mongreldb
```

See [Node.js Quick Start](03-nodejs-quickstart.md) for details.

## Your First Database

Here's a complete Rust program that creates a database, writes data, and reads
it back:

```rust
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Db, Value};

fn main() {
    // 1. Define your table structure (called a "schema")
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
                name: "name".into(),
                ty: TypeId::Bytes,    // strings are stored as bytes
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "age".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
    };

    // 2. Create the database on disk
    let mut db = Db::create("./my_data", schema, 1).unwrap();

    // 3. Write a row
    let row_id = db.put(vec![
        (1, Value::Int64(1)),
        (2, Value::Bytes(b"Alice".to_vec())),
        (3, Value::Int64(30)),
    ]).unwrap();

    // 4. Make it durable (saves to disk)
    db.commit().unwrap();

    // 5. Read it back
    let snap = db.snapshot();
    let row = db.get(row_id, snap).unwrap();
    println!("Name: {:?}", row.columns.get(&2));  // prints "Alice"

    // 6. Get the row count (instant - doesn't scan the data)
    println!("Row count: {}", db.count());
}
```

Run it:

```sh
cargo run --release
```

## Key Concepts

### Rows and Columns

A MongrelDB table has a fixed set of columns defined by a schema. Each column
has a type (`Int64`, `Float64`, `Bytes` for text/binary, `Bool`). Each row has
a unique `RowId` - an internal ID assigned by the database.

### Writes Are Append-Only

When you write or update a row, MongrelDB doesn't modify the old data in place.
Instead, it appends the new version to a write-ahead log (WAL). This is what
makes writes so fast - there's no random-access disk I/O, just sequential
appends. Old data gets cleaned up later during a process called compaction.

### Commits

After calling `put()` or `delete()`, your changes are in memory but not yet
guaranteed to survive a crash. Calling `commit()` flushes the WAL to disk with
`fsync`. You can also call `flush()`, which commits and then moves data from the
WAL into the columnar storage format.

### Snapshots

Reading always happens at a specific "snapshot" - a point-in-time view of the
data. This means writes happening while you read don't affect your results.
Get a snapshot with `db.snapshot()`, then pass it to read methods.

## Next Steps

- [Rust Quick Start](02-rust-quickstart.md) - full API walkthrough
- [Node.js Quick Start](03-nodejs-quickstart.md) - JavaScript/TypeScript guide
- [SQL Queries](04-sql-queries.md) - running SQL with DataFusion
- [Indexes](06-indexes.md) - choosing the right index for your queries
- [Encryption](07-encryption.md) - protecting data at rest
