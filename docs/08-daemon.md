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
VERSION=v0.56.0
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

# Open a credential-enforced database and require HTTP Basic authentication.
MONGRELDB_DB_USERNAME=admin \
MONGRELDB_DB_PASSWORD='database-password' \
mongreldb-server ./my_database 8453 --auth-users
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
docker pull ghcr.io/visorcraft/mongreldb-server:v0.56.0

# Run with auto-restart
docker run -d \
  --name mongreldb \
  --restart=always \
  -p 8453:8453 \
  -v ./my_database:/data \
  ghcr.io/visorcraft/mongreldb-server:v0.56.0 \
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
[Credential Enforcement](15-credential-enforcement.md)), database-open
authentication and HTTP authentication are separate layers.

Set the database-handle credentials as a pair in the process environment:

```sh
MONGRELDB_DB_USERNAME=admin \
MONGRELDB_DB_PASSWORD='database-password' \
mongreldb-server ./my_database 8453 --auth-users
```

The daemon reads both variables once, removes both from its environment before
daemonization or worker-thread startup, opens the database, and zeroizes its
password buffer immediately after the open returns. There is deliberately no
database-password command-line flag because process arguments are commonly
visible to other local users and process monitors. Inject the variables from a
restricted service-manager secret or environment file. Setting only one,
either to an empty value, or using invalid UTF-8 is a startup error.

For an existing `require_auth` database, the variables authenticate the
daemon's database handle. For a database with no catalog yet, they atomically
create a credential-enforced database with that user as the first admin. They
also compose with `--passphrase` for encrypted databases.

The HTTP boundary must independently configure `--auth-users`,
`--auth-token`, or both. The binary refuses to start a `require_auth` database
without either mode, and the library router rejects every route if constructed
that way. Token-only HTTP mode is valid when the database handle was opened
with the current admin credentials above: bearer requests execute as that
exact admin principal. With `--auth-users`, every request's HTTP Basic
credentials are atomically verified and resolved against the current catalog,
then that exact principal is checked at the storage layer. Dropping and
recreating a username does not let the new identity inherit the old identity's
queries, sessions, cursors, or idempotency receipts. An under-privileged
principal receives `403 Forbidden`.

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
    "timeout_ms": 30000,
    "max_output_rows": 10000,
    "max_output_bytes": 8388608
  }'
# Response includes X-MongrelDB-Query-ID.
```

Clients should generate the 32-hex-character query ID with a cryptographically
secure random generator before sending a buffered request. Predictable IDs are
not safe cancellation capabilities. Body `query_id` and `timeout_ms` values
take precedence over `X-MongrelDB-Query-ID` and `X-MongrelDB-Timeout-Ms`
headers.

`max_output_rows` and `max_output_bytes` must be positive. They apply to JSON,
buffered Arrow, and Arrow streams and are clamped to the daemon-configured
maxima.

#### Retried SQL writes

Buffered JSON writes accept `idempotency_key` in the body or
`Idempotency-Key` in the header. If both are supplied, they must match. Keyed
reads are rejected. Keyed requests must contain exactly one supported durable
write:
`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, or the supported table, view, index,
trigger, and policy DDL forms. Here, view DDL means durable materialized views;
ordinary `CREATE VIEW` and `DROP VIEW` are session-scoped and therefore
rejected for idempotent daemon writes. Multi-statement SQL, transaction controls, and
transient/session commands such as `NOTIFY`, `LISTEN`, `ATTACH`, `DETACH`,
`SHOW`, `EXPLAIN`, and `PRAGMA` are rejected before an intent is persisted.

```sh
curl -X POST http://127.0.0.1:8453/sql \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: create-event-42" \
  -d '{"sql":"INSERT INTO events (id) VALUES (42)"}'
```

The key is owner-bound and also binds the normalized, literal-aware SQL
fingerprint, the parameter-list hash (currently the empty list because `/sql`
has no separate bind array), effective output options, and pooled-session
identity, plus the server-enforced receipt expiry policy. Reuse with different
semantics or a different expiry policy returns
`IDEMPOTENCY_KEY_REUSE_MISMATCH`. Every successful keyed write, including a
write that matches no rows, returns a durable receipt instead of result rows.
Retrying returns that receipt without parsing or executing the SQL. A receipt
is HTTP 200 even when post-commit cancellation or serialization failed;
inspect `status`, `outcome`, and `terminal_error` rather than treating HTTP
success as clean response completion. Receipt and intent files contain
HMAC-authenticated hashes and outcome metadata, never the raw key, owner, SQL,
parameters, or result rows. Encrypted databases derive the HMAC key from the
database KEK. Plain databases create a random, database-local key at
`_meta/server-idempotency.key`; it is not derived from an auth token, username,
or password.

Before execution the daemon durably publishes an intent. It durably publishes
the receipt after a known successful terminal outcome, including a commit or a
no-op. Unix uses a file fsync, atomic rename, and parent-directory fsync;
Windows uses a file flush and write-through atomic replacement.
These cannot be one atomic filesystem/database operation. If a process or
power failure occurs between commit and receipt persistence, the intent
remains and the same key returns non-retryable
`QUERY_OUTCOME_UNKNOWN`; the daemon never re-executes that write. Verify the
database outcome before operator recovery. Completed receipts expire after
`MONGRELDB_SQL_IDEMPOTENCY_TTL_SECS`; indeterminate intents do not expire into
unsafe re-execution. `MONGRELDB_SQL_IDEMPOTENCY_MAX_ENTRIES` bounds all unique
persisted scopes, including receipts, live executions, crash-left intents, and
corrupt entries. Each owner is limited to one quarter of that capacity, with a
minimum of one slot, so one tenant cannot consume the whole store. The daemon
fails closed with a capacity error when either limit is reached. Crash-left
intents remain durable outcome-unknown tombstones and continue consuming
capacity until an operator verifies the database outcome and performs recovery.
They cannot safely auto-expire into key reuse: without an atomic database
receipt, expiry would risk repeating a write that committed before the crash.

SQL idempotency format v5 and Kit idempotency format v3 replaced older
unkeyed checksums. Existing older entries fail closed as outcome-unknown; they
are never accepted as replays. The stores use descriptor-relative, no-follow
filesystem operations for directories, entries, and capacity locks. A
symlink, non-regular entry, forged JSON document, failed authentication tag,
or unavailable integrity key blocks execution. Atomic receipt publication uses
a fresh collision-resistant temporary name, so an old fixed `.tmp` path cannot
redirect or replace a receipt.

Filesystem permissions remain the trust boundary. The plain-database integrity
key is created as an owner-only file on Unix, but a process running as the
database owner that can read that key can authenticate its own replacement
entries. Encrypted databases do not persist this server key; they derive it
from the in-memory database KEK. Run the daemon under a dedicated OS account
and do not grant other processes write access to the database root.

`QUERY_OUTCOME_UNKNOWN` responses and retained statuses encode `committed` and
all commit/statement counters as JSON `null`. Clients must preserve that
tri-state value. Only explicit `false` proves no commit.

#### Retained SQL pagination

JSON reads can opt into a process-local retained snapshot. The request must be
exactly one query and must name every output column that may be returned.

```sh
curl -X POST http://127.0.0.1:8453/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql":"SELECT id, created_at, large_payload FROM events ORDER BY id",
    "max_output_rows":100000,
    "max_output_bytes":67108864,
    "pagination":{
      "page_size_rows":100,
      "projection":["id","created_at"],
      "max_page_bytes":262144,
      "max_page_tokens":65536
    }
  }'

curl -X POST http://127.0.0.1:8453/sql/continue \
  -H "Content-Type: application/json" \
  -d '{"cursor":"sp1:..."}'
```

`max_output_rows` and `max_output_bytes` cap the complete retained projected
result. Page limits cap each response. Each response reports exact projected
JSON bytes and `estimated_tokens = ceil(bytes / 4)`; this is a transport hint,
not a model tokenizer. The global retained-memory limit also charges a
conservative per-row/per-column allocation overhead, not only JSON text bytes.
Cursors are signed, owner-bound, server-instance-bound,
and expire with the retained result. Repeating a cursor returns the same page.
Writes cannot create cursors. The SQL engine may compute unprojected columns,
but only the named projection is serialized, retained, and returned. A daemon
restart invalidates all cursors.

The Rust remote client exposes both protocols directly:

```rust,no_run
use mongreldb_client::{MongrelClient, SqlPageOptions};

let client = MongrelClient::new("http://127.0.0.1:8453")?;
let receipt = client.sql_write_idempotent(
    "INSERT INTO events (id) VALUES (42)",
    "create-event-42",
)?;

let first = client.sql_page(
    "SELECT id, created_at FROM events ORDER BY id",
    SqlPageOptions::new(100, vec!["id".into(), "created_at".into()]),
)?;
if let Some(cursor) = first.next_cursor.as_deref() {
    let second = client.continue_sql_page(cursor)?;
    // consume second.rows
}
# Ok::<(), mongreldb_client::ClientError>(())
```

`AsyncMongrelClient` provides async methods with the same names. Client-side
validation rejects empty or oversized keys, cursors, projections, and zero
limits before network I/O.

```sh
# Request cancellation from another connection.
curl -X POST \
  http://127.0.0.1:8453/queries/00112233445566778899aabbccddeeff/cancel

# Inspect safe status metadata. Raw SQL is never returned.
curl http://127.0.0.1:8453/queries/00112233445566778899aabbccddeeff

# Negotiate support instead of guessing from a version string.
curl http://127.0.0.1:8453/capabilities
```

Cancellation capability version 2 accepts an owner-bound cancellation before
the matching SQL request registers. The later request is cancelled before SQL
admission or parsing. Send the same `X-Session-ID` on the cancel request when
the SQL belongs to a pooled session. Pre-registration cancellations are
process-local, bounded, and short-lived; a daemon restart clears them. Admin
metrics expose only their current entry and byte counts, never IDs or SQL.
Unknown cancel requests also use a bounded per-owner fixed-window rate limit,
including repeated requests for the same query ID. Rate-limit exhaustion
returns HTTP 429.

Status includes safe timing trace fields for queueing, planning, execution,
and serialization, the cancel-requested and cancel-observed phases, and the
commit-fence outcome. It never includes raw SQL or parameters.

Query status and cancellation are owner-or-admin operations. Unknown status
lookups and not-owned IDs return 404 with `QUERY_NOT_FOUND`; a valid unknown
cancel request creates the owner/session-bound pre-cancellation above.
Cancellation after the durable commit fence returns 409 with
`CANCEL_TOO_LATE`. A client transport timeout or disconnected socket does not
by itself prove that a buffered server query stopped. Official clients send a
separate cancellation request.

SQL execution limits use these environment variables:

```text
MONGRELDB_SQL_DEFAULT_TIMEOUT_MS
MONGRELDB_SQL_MAX_TIMEOUT_MS
MONGRELDB_SQL_MAX_CONCURRENT
MONGRELDB_SQL_MAX_ACTIVE_QUERIES
MONGRELDB_SQL_FINISHED_QUERY_TTL_SECS
MONGRELDB_SQL_PRE_CANCEL_TTL_MS
MONGRELDB_SQL_PRE_CANCEL_MAX_ENTRIES
MONGRELDB_SQL_PRE_CANCEL_MAX_BYTES
MONGRELDB_SQL_PRE_CANCEL_MAX_PER_OWNER
MONGRELDB_SQL_PRE_CANCEL_RATE_WINDOW_MS
MONGRELDB_SQL_PRE_CANCEL_RATE_PER_OWNER
MONGRELDB_SQL_CANCEL_GRACE_MS
MONGRELDB_SQL_MAX_OUTPUT_BYTES
MONGRELDB_SQL_MAX_OUTPUT_ROWS
MONGRELDB_SQL_IDEMPOTENCY_TTL_SECS
MONGRELDB_SQL_IDEMPOTENCY_MAX_ENTRIES
MONGRELDB_SQL_PAGE_TTL_SECS
MONGRELDB_SQL_PAGE_MAX_ENTRIES
MONGRELDB_SQL_PAGE_MAX_RETAINED_BYTES
MONGRELDB_SQL_PAGE_MAX_PER_OWNER
```

The query deadline starts before the SQL semaphore. Closing a session cancels
its queued and active queries. Graceful server shutdown rejects new SQL,
cancels queued and running work, lets commit-critical writes finish, and
records tasks that exceed cancellation grace.

Prepared-statement `DELETE /sessions/{id}/statements/{name}` accepts the same
`X-MongrelDB-Query-ID` and `X-MongrelDB-Timeout-Ms` controls as `/sql` and
returns `X-MongrelDB-Query-ID`.

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

Keyed `/kit/txn`, trigger DDL, and procedure-call writes use a shared durable
idempotency store. Keys must contain 1 to 256 bytes. Before execution the
daemon fsyncs an intent bound to the authenticated owner, operation, and exact
payload. After execution it fsyncs the exact HTTP status and JSON response.
Reusing a key with different input returns `IDEMPOTENCY_KEY_REUSE_MISMATCH`.
If the commit outcome or receipt publication is uncertain, the intent remains
and every retry returns `QUERY_OUTCOME_UNKNOWN` without re-executing the write.

Basic-auth ownership uses the user's immutable catalog ID and creation epoch,
not the username. Dropping and recreating the same username creates a different
owner that cannot access old sessions, query statuses, continuation cursors, or
idempotency receipts. Bearer ownership uses a domain-separated SHA-256 digest;
the token itself is never stored. `/kit/txn` checks current permissions before
receipt lookup and binds the current security version plus every target table's
table and schema IDs. Permission changes or drop-and-recreate table changes
therefore cannot replay an old response.

The store uses `MONGRELDB_SQL_IDEMPOTENCY_TTL_SECS` (24 hours by default) for
completed receipts and `MONGRELDB_SQL_IDEMPOTENCY_MAX_ENTRIES` (4096 by
default) for the global capacity; one owner may use at most one quarter of the
global capacity. Indeterminate intents do not expire automatically. Full or
unavailable stores reject a keyed write before execution. Legacy unverified
Kit v2 and `_idem/*.json` cache files, and SQL v4 entries, fail closed after
upgrade; archive or remove them only after all retry windows from the older
daemon have safely expired.

### Row-Level Operations

```sh
# Put a row
curl -X POST http://127.0.0.1:8453/tables/events/put \
  -H "Content-Type: application/json" \
  -d '{"row": [1, 42, 2, "alice@test.com", 3, 95.5]}'

# Count rows
curl http://127.0.0.1:8453/tables/events/count

# Commit pending writes
curl -X POST http://127.0.0.1:8453/tables/events/commit
```

The legacy Rust client's `put` method uses exact tagged JSON for values JSON
cannot represent safely: binary bytes and UUID/JSON payloads use lowercase
hex, decimals use a canonical unscaled integer string, and intervals use
canonical component strings. Embeddings and floats must be finite. Invalid
UTF-8/JSON, non-finite numbers, malformed tags, and wrong embedding dimensions
are rejected instead of being converted to `NULL` or lossy text.

Successful legacy transactions and table, procedure, and trigger drops return
`status: "committed"` with matching numeric `epoch` and canonical
`epoch_text`. A client that loses or cannot validate a write response reports
`QUERY_OUTCOME_UNKNOWN`; it never reports a plain transport/decode error as a
known abort.

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

let mut follower = ReplicationFollower::new("http://leader:8453", "/local/copy")?;
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
concurrency limit. Indexes and caches stay warm between requests.

## Security

The daemon listens on `127.0.0.1` by default. For production:

1. Use `--auth-token` for service-to-service traffic, or `--auth-users` for
   per-identity credentials (see **[Users, Roles & Permissions](14-auth.md)**).
2. Use `--max-connections` to prevent resource exhaustion.
3. Put a TLS-terminating reverse proxy (nginx, Caddy) in front for HTTPS.
