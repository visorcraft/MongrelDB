# Daemon Mode (mongrelDB-server)

By default, MongrelDB runs embedded inside your application. But sometimes
you want a long-lived database process that multiple applications can share -
like a traditional database server, but lightweight.

The `mongreldb-server` daemon solves this. It opens the database once, keeps
indexes and caches warm in memory, and serves queries over HTTP.

## Installation

```sh
# Install from crates.io
cargo install mongreldb-server

# Or install a release binary
VERSION=v0.43.3
ASSET=mongreldb-server-linux-x64 # use mongreldb-server-linux-arm64 on ARM64 Linux
curl -L -o /usr/local/bin/mongreldb-server \
  "https://github.com/visorcraft/MongrelDB/releases/download/${VERSION}/${ASSET}"
chmod +x /usr/local/bin/mongreldb-server

# Or build from source
cd crates/mongreldb-server
cargo build --release
```

## Starting the Daemon

```sh
# Basic: start serving a database on the default port (8453)
mongreldb-server ./my_database 8453

# With authentication (Bearer token required on all requests)
mongreldb-server ./my_database 8453 --auth-token my-secret-token

# With connection limit (max 100 concurrent requests)
mongreldb-server ./my_database 8453 --auth-token my-secret-token --max-connections 100
```

The daemon opens the database, builds indexes (if needed), and starts
listening for HTTP requests on `127.0.0.1:8453`.

## History retention

The daemon defaults to 1024 retained commit epochs. Override startup behavior
with `MONGRELDB_HISTORY_RETENTION_EPOCHS`. Authenticated administrators can
inspect or change the durable window while the daemon runs:

```http
GET /history/retention
PUT /history/retention
Content-Type: application/json

{"history_retention_epochs": 1024}
```

Both responses contain `history_retention_epochs` and
`earliest_retained_epoch`. The routes require `ADMIN` permission when catalog
authentication is enabled. Increasing the window cannot restore history that
was already pruned.

## Running as a daemon (--daemon mode)

The `--daemon` flag forks the server into the background, detaches from the
terminal, and writes a PID file:

```sh
# Start in background with a PID file
mongreldb-server ./my_database --daemon

# Custom PID file location
mongreldb-server ./my_database --daemon --pidfile /var/run/mongreldb.pid

# With auth + encryption
mongreldb-server ./my_database --daemon --port 8453 --auth-token my-secret --passphrase my-encryption-key
```

The server handles `SIGINT` (Ctrl+C) and `SIGTERM` gracefully - it flushes all
tables, writes pending data to disk, removes the PID file, and exits with code 0.

## Keeping the daemon running (auto-restart)

For production deployments, use a process supervisor to ensure the daemon
restarts automatically if it crashes or the host reboots.

### systemd (recommended for Linux)

Install the systemd unit file (shipped at
`crates/mongreldb-server/mongreldb-server.service`):

```sh
# Copy the binary and unit file
sudo cp mongreldb-server /usr/local/bin/
sudo cp mongreldb-server.service /etc/systemd/system/

# Edit the unit file to match your database path and auth settings
sudo nano /etc/systemd/system/mongreldb-server.service

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable mongreldb-server
sudo systemctl start mongreldb-server

# Check status
sudo systemctl status mongreldb-server

# View logs
sudo journalctl -u mongreldb-server -f
```

The unit file template:
```ini
[Unit]
Description=MongrelDB Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/mongreldb-server /var/lib/mongreldb 8453
Restart=always
RestartSec=3
User=mongreldb
Group=mongreldb
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

With `Restart=always`, systemd restarts the daemon within 3 seconds if it
crashes, and automatically starts it on boot.

### Docker

```sh
# Pull the multi-arch release image
docker pull ghcr.io/visorcraft/mongreldb-server:v0.43.3

# Run with auto-restart
docker run -d \
  --name mongreldb \
  --restart=always \
  -p 8453:8453 \
  -v ./my_database:/data \
  ghcr.io/visorcraft/mongreldb-server:v0.43.3 \
  /data --port 8453 --auth-token my-secret
```

The `--restart=always` policy restarts the container on crash, daemon exit,
or host reboot (when Docker itself starts).

### supervisord

```ini
[program:mongreldb]
command=/usr/local/bin/mongreldb-server /var/lib/mongreldb --port 8453
directory=/var/lib/mongreldb
autostart=true
autorestart=true
startsecs=3
stderr_logfile=/var/log/mongreldb/error.log
stdout_logfile=/var/log/mongreldb/output.log
user=mongreldb
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mongreldb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mongreldb
  template:
    metadata:
      labels:
        app: mongreldb
    spec:
      containers:
      - name: mongreldb
        image: mongreldb-server:latest
        args: ["/data", "--port", "8453"]
        ports:
        - containerPort: 8453
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: mongreldb-data
```

Kubernetes restarts pods automatically via its health checks and
self-healing mechanisms.

## Authentication

The daemon supports three auth modes - they can be combined:

1. **Token** (`--auth-token <token>`): every request must carry
   `Authorization: Bearer <token>`. A single string compare - fast.
2. **User** (`--auth-users`): every request must carry
   `Authorization: Basic <base64(user:pass)>` against a catalog user
   (Argon2id-verified). The matching `Principal` is injected into request
   extensions for permission checks.
3. **Both** (`--auth-token` **and** `--auth-users`): token **or** valid user
   credentials accepted.

When no flag is set (the default), auth is disabled for local development.

```sh
# Token only - fastest for service-to-service traffic.
mongreldb-server ./my_database 8453 --auth-token my-secret-token

# User auth - per-identity credentials, verified against the catalog.
mongreldb-server ./my_database 8453 --auth-users

# Both - token OR valid user accepted.
mongreldb-server ./my_database 8453 --auth-token my-secret-token --auth-users --max-connections 100
```

```sh
# Bearer token
curl -H "Authorization: Bearer my-secret-token" http://127.0.0.1:8453/health
# → "ok"

# Basic auth against a catalog user
curl -u alice:s3cret-pw http://127.0.0.1:8453/health
# → "ok"

# No credentials
curl http://127.0.0.1:8453/health
# → 401 Unauthorized
```

Manage users on a running daemon through the SQL endpoint (or any of the
other surfaces documented in **[Users, Roles & Permissions](14-auth.md)**):

```sh
# Create the first admin user before enabling --auth-users in production.
curl -X POST http://127.0.0.1:8453/sql \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE USER alice WITH PASSWORD '\''s3cret-pw'\''; ALTER USER alice ADMIN"}'
```

### require_auth databases

When a database has `require_auth = true` (see
[Credential Enforcement](15-credential-enforcement.md)), the daemon **must**
run with `--auth-users` (or `--auth-users` plus `--auth-token`). Token-only
mode (`--auth-token` without `--auth-users`) is insufficient because it does
not resolve a catalog `Principal` - the storage layer needs a real user to
check per-operation permissions.

With `--auth-users`, each request's HTTP Basic credentials are verified against
the catalog and the resolved `Principal` is checked at the storage layer too
(defense in depth). A request that passes the HTTP gate but maps to an
under-privileged principal will be rejected by the storage layer with `403
Forbidden`.

## Connection Pooling

When `--max-connections N` is set, the daemon caps concurrent in-flight
requests via a `ConcurrencyLimitLayer`. Requests beyond the limit wait in a
queue. Default: unlimited (all requests handled immediately).

## API Endpoints

All requests use JSON for parameters. Query results come back as Arrow IPC
bytes (a binary format for columnar data).

### Health Check

```sh
curl http://127.0.0.1:8453/health
# → "ok"
```

### Table Management

```sh
# List tables
curl http://127.0.0.1:8453/tables

# Create a table
curl -X POST http://127.0.0.1:8453/tables \
  -H "Content-Type: application/json" \
  -d '{"name": "events", "columns": [...]}'

# Drop a table
curl -X DELETE http://127.0.0.1:8453/tables/events
```

### SQL

```sh
curl -X POST http://127.0.0.1:8453/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT count(*) FROM events WHERE amount > 500",
    "format": "arrow",
    "query_id": "00112233445566778899aabbccddeeff",
    "timeout_ms": 30000
  }'
# Response includes X-MongrelDB-Query-ID.
```

Clients should generate the 32-hex-character query ID before sending a
buffered request. Body `query_id` and `timeout_ms` values take precedence over
`X-MongrelDB-Query-ID` and `X-MongrelDB-Timeout-Ms` headers.

```sh
# Request cancellation from another connection.
curl -X POST \
  http://127.0.0.1:8453/queries/00112233445566778899aabbccddeeff/cancel

# Inspect safe status metadata. Raw SQL is never returned.
curl http://127.0.0.1:8453/queries/00112233445566778899aabbccddeeff

# Negotiate support instead of guessing from a version string.
curl http://127.0.0.1:8453/capabilities
```

Query status and cancellation are owner-or-admin operations. Unknown and
not-owned IDs both return 404. Cancellation after the durable commit fence
returns 409 with `CANCEL_TOO_LATE`. A client transport timeout or disconnected
socket does not by itself prove that a buffered server query stopped. Official
clients send a separate best-effort cancellation request.

SQL execution limits use these environment variables:

```text
MONGRELDB_SQL_DEFAULT_TIMEOUT_MS
MONGRELDB_SQL_MAX_TIMEOUT_MS
MONGRELDB_SQL_MAX_CONCURRENT
MONGRELDB_SQL_MAX_ACTIVE_QUERIES
MONGRELDB_SQL_FINISHED_QUERY_TTL_SECS
MONGRELDB_SQL_CANCEL_GRACE_MS
MONGRELDB_SQL_MAX_OUTPUT_BYTES
MONGRELDB_SQL_MAX_OUTPUT_ROWS
```

The query deadline starts before the SQL semaphore. Closing a session cancels
its queued and active queries. Graceful server shutdown rejects new SQL,
cancels queued and running work, lets commit-critical writes finish, and
records tasks that exceed cancellation grace.

### Typed Kit API

The daemon serves a typed Kit API with authoritative constraint enforcement:

> **Compatibility note:** `default_value` is interpreted as a literal JSON
> scalar. The legacy behavior of treating `default_value: "now"` or
> `default_value: "uuid"` as dynamic defaults has been removed. Use
> `default_expr: "now"` or `default_expr: "uuid"` for dynamic defaults.

```sh
# Get the full schema catalog
curl http://127.0.0.1:8453/kit/schema

# Atomic typed write batch (put/upsert/delete with idempotency keys)
curl -X POST http://127.0.0.1:8453/kit/txn \
  -H "Content-Type: application/json" \
  -d '{"operations": [...], "idempotency_key": "..."}'

# Typed query with conditions
curl -X POST http://127.0.0.1:8453/kit/query \
  -H "Content-Type: application/json" \
  -d '{"table": "events", "conditions": [...], "limit": 1000, "offset": 10000}'
```

### Row-Level Operations

```sh
# Put a row
curl -X POST http://127.0.0.1:8453/tables/events/put \
  -H "Content-Type: application/json" \
  -d '{"row": [[1, 42], [2, "alice@test.com"], [3, 95.5]]}'

# Count rows
curl http://127.0.0.1:8453/tables/events/count

# Commit pending writes
curl -X POST http://127.0.0.1:8453/tables/events/commit
```

### Procedures and Triggers

```sh
# List/create/drop/call stored procedures
curl http://127.0.0.1:8453/procedures
curl -X POST http://127.0.0.1:8453/procedures -d '...'
curl -X POST http://127.0.0.1:8453/procedures/my_proc/call -d '{"args": {...}}'

# List/create/drop triggers
curl http://127.0.0.1:8453/triggers
```

### Compaction

```sh
# Compact all tables
curl -X POST http://127.0.0.1:8453/compact

# Compact one table
curl -X POST http://127.0.0.1:8453/tables/events/compact
```

The daemon also runs a **background auto-compactor** that sweeps every
30 seconds and merges any table with 8+ sorted runs.

## Change Data Capture (NOTIFY / LISTEN)

The daemon publishes change events to a broadcast channel. Applications can
subscribe via the `GET /events` endpoint, which streams events as
newline-delimited JSON (`ChangeEvent { channel, table, op, epoch, message }`):

```sh
# Stream change events
curl http://127.0.0.1:8453/events
# {"channel":"","table":"events","op":"put","epoch":5,"message":null}
# {"channel":"alerts","table":"","op":"notify","epoch":6,"message":"threshold exceeded"}
```

SQL `NOTIFY channel [, 'payload']` publishes a notification on a named
channel; `LISTEN channel` is accepted (subscribers connect via `/events`).

## Replication (WAL Streaming)

The daemon exposes `GET /wal/stream?since=<seq>` which streams committed WAL
records as newline-delimited JSON (`{ seq, txn_id, op }`). A follower
polls this endpoint and applies records to a local database copy:

```rust
use mongreldb_client::ReplicationFollower;

let mut follower = ReplicationFollower::new("http://leader:8453", "/local/copy");
let n = follower.sync()?;  // fetch + count new records
println!("applied {n} records, up to seq {}", follower.last_seq());
```

This enables async leader→follower replication for read scaling and disaster
recovery.

## Connecting from Rust

```rust
use mongreldb_client::MongrelClient;

let client = MongrelClient::builder("http://127.0.0.1:8453")
    .bearer_token("token")
    .build()?;

// SQL
let batches = client.sql("SELECT * FROM events WHERE score > 90")?;

// Row count
let count = client.count("events")?;

// Typed Kit operations
let schema = client.kit_schema()?;
```

Use `.basic_auth(username, password)` for HTTP Basic. `AsyncMongrelClient`
provides the same builders and typed AI routes for Tokio applications.

## Connecting from Node.js

```javascript
const { RemoteDatabase } = require('@visorcraft/mongreldb');

const db = new RemoteDatabase('http://127.0.0.1:8453');

const count = db.count('events');
const arrowBytes = db.sql('SELECT * FROM events LIMIT 100');

// Compact tables remotely
db.compact();
db.compactTable('events');
```

## How It Works

The daemon holds the `Database` open with all indexes in memory. HTTP
requests are handled asynchronously (axum + tokio), with an optional
concurrency limit. Because the indexes and caches stay warm between requests,
repeated queries are fast - warm result-cache hits return in ~0.1 µs.

## Security

The daemon listens on `127.0.0.1` by default. For production:

1. Use `--auth-token` for service-to-service traffic, or `--auth-users` for
   per-identity credentials (see **[Users, Roles & Permissions](14-auth.md)**).
2. Use `--max-connections` to prevent resource exhaustion.
3. Put a TLS-terminating reverse proxy (nginx, Caddy) in front for HTTPS.
