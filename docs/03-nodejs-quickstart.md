# Node.js Quick Start

MongrelDB ships as a native Node.js addon (NAPI). It runs in-process — no
database server, no HTTP, no network. This means write latency stays at
single-digit microseconds.

## Installation

```sh
cd crates/mongreldb-node
npm install
npx napi build --release --platform
```

This generates:
- `mongreldb.<platform>.node` — the native binary
- `index.js` — JavaScript entry point
- `index.d.ts` — TypeScript type definitions

Copy these into your project, or add the crate directory to your `package.json`.

## Creating a Database

```javascript
const { Database, ColumnType, IndexKindSpec } = require('./index.js');

const db = new Database('./my_data', {
    columns: [
        { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
        { id: 2, name: 'email', ty: ColumnType.Bytes, primaryKey: false, nullable: false },
        { id: 3, name: 'score', ty: ColumnType.Float64, primaryKey: false, nullable: false },
    ],
    indexes: [
        { name: 'email_bm', columnId: 2, kind: IndexKindSpec.Bitmap },
    ],
}, 1);
```

To reopen an existing database:

```javascript
const db = Database.open('./my_data');
```

## Writing Data

### Single Row (Sync)

```javascript
const rowId = db.put([
    { columnId: 1, int64: 42n },
    { columnId: 2, text: 'alice@example.com' },
    { columnId: 3, float64: 98.5 },
]);
console.log('Row ID:', rowId);  // BigInt

db.commit();  // make it durable
```

### Single Row (Async)

```javascript
const rowId = await db.putAsync([
    { columnId: 1, int64: 43n },
    { columnId: 2, text: 'bob@example.com' },
    { columnId: 3, float64: 87.0 },
]);
await db.commitAsync();
```

### Batch Insert

For many rows at once — one FFI call instead of N:

```javascript
const rows = [];
for (let i = 0n; i < 10000n; i++) {
    rows.push([
        { columnId: 1, int64: i },
        { columnId: 2, text: `user${i}@test.com` },
        { columnId: 3, float64: Number(i) * 1.5 },
    ]);
}

const rowIds = db.putBatch(rows, true);  // true = commit immediately
console.log(`Inserted ${rowIds.length} rows`);
```

### Bulk Load (Fastest)

For millions of rows, use typed arrays — data crosses the FFI boundary as raw
bytes with zero per-row conversion:

```javascript
const n = 1_000_000;

db.bulkLoadTyped([
    {
        columnId: 1,
        ty: ColumnType.Int64,
        data: Buffer.from(new BigInt64Array(n).map((_, i) => BigInt(i)).buffer),
        validity: Buffer.alloc(n / 8, 0xFF),
    },
    // ... other columns
]);
```

## Reading Data

### Get a Single Row

```javascript
const row = db.get(42n);  // BigInt row ID
console.log(row.cells);   // array of cell values
console.log(row.rowId);   // BigInt
```

### Get Row Count

```javascript
console.log('Rows:', db.count());  // O(1), instant
```

### Query with Conditions

```javascript
const results = db.query([
    { kind: 'bitmapEq', columnId: 2, value: Buffer.from('alice@example.com') },
]);

console.log(`Found ${results.length} rows`);
for (const row of results) {
    console.log(row);
}
```

### Query Returning Arrow (Zero-Copy)

Instead of JavaScript objects, get Arrow IPC bytes — consumable by the
`apache-arrow` npm package with no per-row allocation:

```javascript
const arrowBuffer = db.queryArrow([
    { kind: 'rangeF64', columnId: 3, lo: 90.0, hi: 100.0 },
]);

// Parse with apache-arrow:
// const { Table } = require('apache-arrow');
// const table = Table.from(arrowBuffer);
```

### Batched Query Results

For large result sets, get one Arrow buffer per ~65K-row page chunk:

```javascript
const chunks = db.queryArrowBatched([
    { kind: 'bitmapEq', columnId: 2, value: Buffer.from('premium') },
]);

for (const chunk of chunks) {
    // process each chunk independently
    // reduces peak memory vs. holding the full result
}
```

## Deleting and Updating

```javascript
db.delete(42n);   // mark row 42 as deleted
db.commit();
```

To update a row, just `put` with the same data — MongrelDB handles versioning
internally.

## Write Buffer (Optional Micro-Batching)

For high-throughput local writes where you don't need immediate durability:

```javascript
const { WriteBuffer } = require('./index.js');

const buf = new WriteBuffer(db, 5000);  // auto-flush every 5000 rows

for (let i = 0n; i < 100_000n; i++) {
    buf.put([
        { columnId: 1, int64: i },
        { columnId: 2, text: `user${i}` },
        { columnId: 3, float64: 0.0 },
    ]);
}

buf.flush();  // writes become durable only after this call
```

**Important:** `WriteBuffer` is the opposite of `put()` — writes are *not*
durable until you call `flush()`. Use it only when you can tolerate losing
buffered data on a crash.

## Connecting to a Daemon

If you're running `mongreldb-server` (see [Daemon Guide](08-daemon.md)), multiple
Node processes can share one warm database:

```javascript
const { RemoteDatabase } = require('./index.js');

const db = new RemoteDatabase('http://127.0.0.1:8453');

const count = db.count();          // queries the daemon
const arrowBytes = db.sql('SELECT * FROM events WHERE score > 90');
db.commit();
```

## TypeScript

Type definitions are generated at build time (`index.d.ts`). All row IDs,
counts, and epochs use `BigInt` (not JS Number) to preserve full 64-bit
precision.

## Closing

```javascript
db.close();   // flush + release resources
```

The database is also cleaned up automatically when garbage-collected, but
calling `close()` ensures data is flushed before exit.
