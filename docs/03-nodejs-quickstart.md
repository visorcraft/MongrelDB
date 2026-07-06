# Node.js Quick Start

MongrelDB ships as a native NAPI addon. It runs in-process, so write and query
latency are not hidden behind an HTTP hop. Build the addon in release mode:

```sh
cd crates/mongreldb-node
npm install
npm run build
```

This generates `mongreldb.<platform>.node`, `native.js`, and `native.d.ts`.
The hand-written `index.js` wrapper re-exports the native API and adds a
retrying transaction helper.

## Create A Database

```javascript
const { Database, ColumnType, IndexKindSpec } = require('./index.js');

const db = Database.withPath('./my_data');

db.createTable('users', {
  columns: [
    { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
    { id: 2, name: 'email', ty: ColumnType.Bytes, primaryKey: false, nullable: false },
    { id: 3, name: 'tier', ty: ColumnType.Bytes, primaryKey: false, nullable: false },
    { id: 4, name: 'score', ty: ColumnType.Float64, primaryKey: false, nullable: false },
  ],
  indexes: [
    { name: 'email_bm', columnId: 2, kind: IndexKindSpec.Bitmap },
    { name: 'tier_bm', columnId: 3, kind: IndexKindSpec.Bitmap },
  ],
});

const users = db.table('users');
```

Reopen an existing database with:

```javascript
const db = Database.open('./my_data');
const users = db.table('users');
```

## Write Rows

```javascript
const inserted = users.put([
  { columnId: 1, int64: 42n },
  { columnId: 2, text: 'alice@example.com' },
  { columnId: 3, text: 'gold' },
  { columnId: 4, float64: 98.5 },
]);

console.log(inserted.rowId); // BigInt
users.commit();              // durable group commit
```

Async variants offload blocking I/O to the NAPI blocking pool:

```javascript
await users.putAsync([
  { columnId: 1, int64: 43n },
  { columnId: 2, text: 'bob@example.com' },
  { columnId: 3, text: 'silver' },
  { columnId: 4, float64: 87.0 },
]);
await users.commitAsync();
```

Batch inserts cross the FFI boundary once and still need a commit:

```javascript
const results = users.putBatch([
  [
    { columnId: 1, int64: 100n },
    { columnId: 2, text: 'user100@example.com' },
    { columnId: 3, text: 'gold' },
    { columnId: 4, float64: 91.0 },
  ],
  [
    { columnId: 1, int64: 101n },
    { columnId: 2, text: 'user101@example.com' },
    { columnId: 3, text: 'bronze' },
    { columnId: 4, float64: 72.0 },
  ],
]);
users.commit();
console.log(results.map((r) => r.rowId));
```

For typed bulk ingest, use a fixed-width table and pass raw little-endian
buffers. `bulkLoadTyped` supports Int64, Float64, and Bool columns; use
`putBatch` for rows that include Bytes or Embedding columns.

```javascript
db.createTable('metrics', {
  columns: [
    { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
    { id: 2, name: 'score', ty: ColumnType.Float64, primaryKey: false, nullable: false },
  ],
  indexes: [],
});

const metrics = db.table('metrics');
const ids = new BigInt64Array([200n, 201n, 202n]);
const scores = new Float64Array([88.0, 93.5, 77.25]);

const epoch = metrics.bulkLoadTyped([
  { columnId: 1, ty: ColumnType.Int64, data: Buffer.from(ids.buffer) },
  { columnId: 2, ty: ColumnType.Float64, data: Buffer.from(scores.buffer) },
]);
console.log(epoch);
```

## Read Rows

```javascript
const row = users.get(0n); // physical row id
console.log(row?.rowId, row?.cells);

console.log(users.count()); // O(1), BigInt
```

Primary-key helpers are available for single-column text and Int64 primary
keys:

```javascript
const byId = users.getByPkInt64(42n);
```

## Query With Conditions

Conditions are ANDed together inside the engine. Use `ConditionKind` constants
from the generated bindings:

```javascript
const { ConditionKind } = require('./index.js');

const matches = users.query([
  { kind: ConditionKind.BitmapIn, columnId: 3, values: ['gold', 'silver'] },
  { kind: ConditionKind.RangeF64, columnId: 4, float64Lo: 80.0, float64Hi: 100.0 },
]);

console.log(matches.length);
```

When you only need cardinality, `countWhere` resolves the same native condition
set without materializing rows when possible:

```javascript
const n = users.countWhere([
  { kind: ConditionKind.BitmapIn, columnId: 3, values: ['gold', 'silver'] },
]);
console.log(n);
```

For columnar consumers, `queryArrow` returns Arrow IPC bytes:

```javascript
const arrowBytes = users.queryArrow([
  { kind: ConditionKind.RangeF64, columnId: 4, float64Lo: 90.0, float64Hi: 100.0 },
]);
```

`BytesPrefix` resolves an anchored `LIKE 'prefix%'` exactly on a bitmap-indexed
`Bytes` column (no residual re-check):

```javascript
// Find rows whose `key` column (Bytes, bitmap-indexed) starts with "user:".
const userRows = db.table('events').query([
  { kind: ConditionKind.BytesPrefix, columnId: 2, text: 'user:' },
]);
```

## Running SQL

`db.sql(sql)` runs a SQL statement through the embedded DataFusion frontend and
returns the result as Arrow IPC bytes (decode with `apache-arrow`'s
`tableFromIPC`). Read statements return rows; DDL/DML (`CREATE TABLE`,
`CREATE VIEW`, `INSERT`, `ANALYZE`, `VACUUM`) return an empty buffer.

```javascript
const { tableFromIPC } = require('apache-arrow');

// Cross-table SQL read → Arrow table.
const users = tableFromIPC(await db.sql('SELECT id, email FROM users WHERE score > 90'));

// DDL: create a view, then query it in a subsequent call.
await db.sql("CREATE VIEW vip AS SELECT id, email FROM users WHERE score >= 90");
const vips = tableFromIPC(await db.sql('SELECT * FROM vip ORDER BY id'));
```

The `Database` caches its SQL session for the database's lifetime, so
session-scoped objects — views, prepared statements, the result cache — persist
across `sql()` calls. Closing and reopening the database starts a fresh session
(re-apply any view-defining migrations then).

## Advanced SQL: CTEs, Windows, Regex, Catalog

The SQL surface (`db.sql()`) runs DataFusion 54, which supports a rich SQL
dialect. These features are all accessible through `await db.sql(sql)`:

**Recursive CTEs** — tree traversal, hierarchy queries:

```javascript
const { tableFromIPC } = require('apache-arrow');

const batches = tableFromIPC(await db.sql(`
  WITH RECURSIVE tree AS (
    SELECT id, parent, 0 AS depth FROM nodes WHERE parent IS NULL
    UNION ALL
    SELECT n.id, n.parent, t.depth + 1
    FROM nodes n JOIN tree t ON n.parent = t.id
  )
  SELECT id, depth FROM tree ORDER BY id
`));
```

**Window functions** — rankings, running totals, time-series:

```javascript
const result = tableFromIPC(await db.sql(`
  SELECT category, amount,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rank,
         SUM(amount) OVER (PARTITION BY category) AS total
  FROM orders
`));
```

**Regex matching** — `regexp('pattern', value)` returns 1 (match) or 0:

```javascript
const matched = tableFromIPC(await db.sql(
  "SELECT id, email FROM users WHERE regexp('^.*@example\\\\.com$', email) = 1"
));
```

**Catalog introspection** — `information_schema.tables` lists tables, views, and triggers:

```javascript
const catalog = tableFromIPC(await db.sql(
  'SELECT type, name FROM information_schema.tables ORDER BY name'
));
```

**Cross-database queries** — `ATTACH` opens a second database:

```javascript
await db.sql("ATTACH '/path/to/other-db' AS other");
const crossDb = tableFromIPC(await db.sql(
  'SELECT id FROM other_users WHERE id > 100'
));
await db.sql('DETACH other');
```

**Sub-transactions** — `SAVEPOINT` within a `BEGIN`/`COMMIT` block:

```javascript
await db.sql('BEGIN');
await db.sql("INSERT INTO users VALUES (1, 'alice@example.com')");
await db.sql('SAVEPOINT sp1');
await db.sql("INSERT INTO users VALUES (2, 'bob@example.com')");
await db.sql('ROLLBACK TO sp1');  // discards bob, keeps alice
await db.sql('COMMIT');
```

## Transactions

Use `begin()` for atomic multi-table staging:

```javascript
const txn = db.begin();
txn.put('users', [
  { columnId: 1, int64: 500n },
  { columnId: 2, text: 'txn@example.com' },
  { columnId: 3, text: 'gold' },
  { columnId: 4, float64: 99.0 },
]);
txn.commit();
```

The wrapper also exposes `db.transaction(fn, opts)` for conflict retries.

## Write Buffer

`WriteBuffer` batches non-durable writes until `flush()`:

```javascript
const { WriteBuffer } = require('./index.js');

const buf = new WriteBuffer(users, 5000);
buf.put([
  { columnId: 1, int64: 900n },
  { columnId: 2, text: 'buffered@example.com' },
  { columnId: 3, text: 'bronze' },
  { columnId: 4, float64: 50.0 },
]);
buf.flush();
```

Use it only when losing buffered writes on crash is acceptable.

## Daemon Client

`RemoteDatabase` talks to `mongreldb-server` over HTTP for multi-process cache
sharing:

```javascript
const { RemoteDatabase } = require('./index.js');

const remote = new RemoteDatabase('http://127.0.0.1:8453');
console.log(remote.count('users'));
const arrow = remote.sql('SELECT * FROM users WHERE score > 90');
```

## Users, Roles & Permissions

Catalog users have Argon2id-hashed passwords and belong to zero or more
roles; each role carries a set of permissions. See
**[Users, Roles & Permissions](14-auth.md)** for the full model.

```javascript
const { Database } = require('./index.js');
const db = Database.open('./my_database');

// Users
db.createUser('alice', 's3cret-pw');
db.alterUserPassword('alice', 'new-pw');
console.log(db.verifyUser('alice', 'new-pw')); // true
db.setUserAdmin('alice', true);
console.log(db.users());                       // ['alice']

// Roles + permissions (string vocabulary: all, admin, ddl, select:table, …)
db.createRole('analyst');
db.grantPermission('analyst', 'select:orders');
db.grantPermission('analyst', 'insert:orders');
db.grantRole('alice', 'analyst');
console.log(db.roles());                       // ['analyst']
```

For the HTTP daemon, start with `--auth-token <token>` (Bearer),
`--auth-users` (HTTP Basic against catalog users), or both. See
**[Daemon Mode](08-daemon.md#authentication)**.

## Closing

```javascript
db.close();
```
