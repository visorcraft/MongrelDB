# mongreldb-node

Native Node.js addon for MongrelDB via [NAPI](https://napi.rs) — the
**better-sqlite3 model**: in-process, no HTTP latency, so the ~6 µs single-row
write isn't dwarfed by a network round-trip. Exposes a **typed object/method
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
  sql(sql: string): Promise<Buffer>
  close(): void
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

### Hybrid query — the differentiator

`query` intersects row-id sets from any combination of the six index types in a
single in-process call — something no HTTP vector DB or SQL FTS pipeline can do
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
