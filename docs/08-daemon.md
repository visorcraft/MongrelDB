# Daemon Mode (mongreldb-server)

By default, MongrelDB runs embedded inside your application. But sometimes
you want a long-lived database process that multiple applications can share —
like a traditional database server, but lightweight.

The `mongreldb-server` daemon solves this. It opens the database once, keeps
indexes and caches warm in memory, and serves queries over HTTP.

## When to Use the Daemon

| Scenario | Embedded | Daemon |
|---|---|---|
| Single application | ✓ | |
| CLI tool that opens, queries, closes | ✓ | |
| Multiple processes sharing one database | | ✓ |
| Web server + background workers | | ✓ |
| Long-running analytics with warm cache | | ✓ |

## Starting the Daemon

Build and run the server:

```sh
cd crates/mongreldb-server
cargo build --release

# Start serving a database on the default port (8453)
./target/release/mongreldb-server ./my_database 8453
```

The daemon opens the database, builds indexes (if needed), and starts
listening for HTTP requests on `127.0.0.1:8453`.

## API Endpoints

All requests use JSON for parameters. Query results come back as Arrow IPC
bytes (a binary format for columnar data).

### Health Check

```sh
curl http://127.0.0.1:8453/health
# → "ok"
```

### Row Count

```sh
curl http://127.0.0.1:8453/count
# → { "count": 1000000 }
```

### SQL Query

```sh
curl -X POST http://127.0.0.1:8453/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT count(*) FROM events WHERE amount > 500"}'
# → Arrow IPC bytes (binary)
```

### Native Condition Query

```sh
curl -X POST http://127.0.0.1:8453/query \
  -H "Content-Type: application/json" \
  -d '{
    "conditions": [
      {"kind": "bitmap_eq", "column_id": 2, "value": "premium"},
      {"kind": "range_f64", "column_id": 3, "lo": 100.0, "hi": 500.0}
    ],
    "projection": [1, 2, 3]
  }'
# → Arrow IPC bytes (binary)
```

### Write Data

```sh
curl -X POST http://127.0.0.1:8453/put \
  -H "Content-Type: application/json" \
  -d '{"row": [[1, 42], [2, "alice@test.com"], [3, 95.5]]}'
# → { "row_id": "1000001" }

curl -X POST http://127.0.0.1:8453/commit
# → { "epoch": 5 }
```

### Delete

```sh
curl -X POST http://127.0.0.1:8453/delete \
  -H "Content-Type: application/json" \
  -d '{"row_id": 42}'
```

### Compaction

```sh
# Compact all tables
curl -X POST http://127.0.0.1:8453/compact
# → {"status":"ok","compacted":3,"skipped":1}

# Compact one table
curl -X POST http://127.0.0.1:8453/tables/events/compact
# → {"status":"compacted","table":"events"}
```

The daemon also runs a **background auto-compactor** that sweeps every
30 seconds and merges any table with 8+ sorted runs — so under steady
write load, query latency stays flat without any manual intervention.
See [Maintenance & Operations](09-maintenance.md) for details.

## Connecting from Rust

```rust
use mongreldb_client::MongrelClient;

let client = MongrelClient::new("http://127.0.0.1:8453");

// SQL
let batches = client.sql("SELECT * FROM events WHERE score > 90")?;

// Row count
let count = client.count()?;

// Write
client.put(vec![(1, Value::Int64(42)), (2, Value::Bytes(b"hello".to_vec())])?;
client.commit()?;
```

## Connecting from Node.js

```javascript
const { RemoteDatabase } = require('./index.js');

const db = new RemoteDatabase('http://127.0.0.1:8453');

const count = db.count();
const arrowBytes = db.sql('SELECT * FROM events LIMIT 100');
db.commit();
```

## How It Works

The daemon holds the `Db` open with all indexes in memory. Every HTTP request
locks the database mutex, executes the query, and returns the result. Because
the indexes and caches stay warm between requests, repeated queries are fast.

The daemon does not currently support concurrent queries (they're serialized
by the mutex). If you need parallelism, run multiple daemon instances on
different ports, each with its own database.

## Security

The daemon listens on `127.0.0.1` only — it's not accessible from other
machines by default. There is no authentication. If you need remote access
or auth, put a reverse proxy (like nginx or Caddy) in front with TLS and
authentication.
