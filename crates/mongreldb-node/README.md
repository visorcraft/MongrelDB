# mongreldb-node

High-performance Node.js bindings for MongrelDB via [NAPI](https://napi.rs),
with native in-process storage, sub-ms writes, and hybrid indexing. It follows
the **better-sqlite3 model**: no HTTP latency, so the ~8 µs single-row write
isn't dwarfed by a network round-trip. Exposes a **typed object/method
interface** (not SQL); TypeScript types are generated at build time.

This crate is built **separately** from the Rust workspace (it targets the NAPI
ABI and needs Node.js tooling). It is excluded from `cargo {test,clippy}
--workspace`.

## Build

Requires Node.js ≥ 16 and `@napi-rs/cli`:

```bash
cd crates/mongreldb-node
npm install
npm run build          # → mongreldb.<platform>.node + index.d.ts
```

## API sketch (generated `index.d.ts`)

```ts
class Database {
  static withPath(path: string): Database
  static open(path: string): Database
  createTable(name: string, schema: SchemaSpec): bigint
  table(name: string): TableHandle
  begin(): Transaction
  createProcedure(spec: ProcedureSpec): bigint
  createOrReplaceProcedure(spec: ProcedureSpec): bigint
  dropProcedure(name: string): void
  procedures(): ProcedureInfo[]
  procedure(name: string): ProcedureInfo | null
  callProcedure(name: string, opts?: ProcedureCallOptions): ProcedureCallResult
  callProcedureAsync(name: string, opts?: ProcedureCallOptions): Promise<ProcedureCallResult>
  sql(sql: string): Promise<Buffer>
  startSql(sql: string, opts?: NativeSqlOptions): NativeSqlQuery
  close(): void
}

class NativeSqlQuery {
  readonly id: string
  cancel(): NativeCancelOutcome
  status(): NativeQueryStatus
  resultArrow(): Promise<Buffer>
  resultRows(): Promise<Array<Record<string, unknown>>>
}

class TableHandle {
  put(cells: Cell[]): PutResult
  putBatch(rows: Cell[][]): PutResult[]
  bulkLoadTyped(columns: TypedColumn[]): bigint
  commit(): bigint
  flush(): bigint
  count(): bigint                        // O(1)
  countWhere(conditions: ConditionSpec[]): bigint
  get(rowId: bigint): RowJs | null
  query(conditions: ConditionSpec[]): RowJs[]   // hybrid: bitmap ∩ range ∩ FM ∩ HNSW
  queryArrow(conditions: ConditionSpec[]): Buffer
}
```

### Controlled SQL

`startSql` keeps execution and result conversion under one registered query.
The deadline and cancellation remain active while Arrow IPC or JavaScript rows
are produced. Output limits default to 1,000,000 rows and 64 MiB and can be
lowered per query:

```ts
const query = db.startSql("SELECT id, payload FROM events", {
  queryId: crypto.randomUUID(),
  timeoutMs: 5_000,
  maxOutputRows: 10_000,
  maxOutputBytes: 8 * 1024 * 1024,
})

const outcome = query.cancel()
const status = query.status()
const rows = await query.resultRows()
```

Cancellation returns `Accepted`, `AlreadyCancelling`, `TooLate`,
`AlreadyFinished`, or `NotFound`. Inspect `status.durableOutcome` before
retrying a write after cancellation, timeout, serialization failure, or lost
transport. `resultRows()` preserves 64-bit integer columns as JavaScript
`BigInt` and binary columns as `Buffer`.

Daemon connections accept shared Bearer or Basic authentication options:

```ts
const bearer = new RemoteDatabase("https://db.example", {
  bearerToken: process.env.MONGRELDB_TOKEN,
  transportTimeoutMs: 10_000,
})
const basic = new RemoteDatabase("https://db.example", {
  username: process.env.MONGRELDB_USER,
  password: process.env.MONGRELDB_PASSWORD,
})

const status = bearer.queryStatus(queryId)
// Durable epochs are returned as lossless BigInt values.
console.log(status.outcomeKnown, status.durableOutcome.lastCommitEpoch)

// Remote sql()/sqlWithOptions() return Promise<Buffer> and run off-thread.
// A handle additionally exposes its generated ID plus bound cancel/status.
const query = bearer.startSql("SELECT * FROM events", { timeoutMs: 5_000 })
const result = query.result() // Promise<Buffer>
query.cancel()               // remains callable while the request runs
await result

const receipt = await bearer.sqlWriteIdempotent(
  "INSERT INTO jobs (id, payload) VALUES (1, 'ready')",
  {
    idempotencyKey: "create-job-1",
    timeoutMs: 5_000,
  },
)
console.log(receipt.committed, receipt.durableOutcome.lastCommitEpoch)
```

Credentials in the URL are rejected. Authentication is attached by one
request path to every daemon route. `sqlWriteIdempotent` is the typed JSON
receipt API for daemon writes and, like the Arrow SQL APIs, runs off the JS
event loop. Regular `sqlWithOptions` always returns Arrow IPC bytes and rejects
`idempotencyKey`. If a SQL response is lost, the client polls the query status,
requests bounded cancellation when it is still active, and throws a structured
error with `remoteQueryError`; compact status records that cannot prove the
durable outcome report `QUERY_OUTCOME_UNKNOWN`.

Native open failures expose stable `error.code` values:
`MONGRELDB_AUTH_REQUIRED`, `MONGRELDB_DATABASE_LOCKED`, and
`MONGRELDB_NOT_FOUND`. Trigger definition failures use `TRIGGER_VALIDATION`.
Applications should branch on these codes, never on message text.

### Hybrid query - the differentiator

`query` intersects row-id sets from any combination of the six index types in a
single in-process call - something no HTTP vector DB or SQL FTS pipeline can do
in one hop:

```ts
db.query([
  { kind: ConditionKind.Ann, columnId: 5, embedding: queryVec, k: 50 },
  { kind: ConditionKind.FmContains, columnId: 2, text: "rome" },
  { kind: ConditionKind.BitmapIn, columnId: 3, values: ["eu", "na"] },
])
```

## Notes

- Row ids / counts / epochs cross the FFI as JS `BigInt` (`u64`), so the full
  64-bit id space is lossless.
- Most blocking table methods also expose `*Async` Promise variants that run on
  the NAPI blocking pool.
