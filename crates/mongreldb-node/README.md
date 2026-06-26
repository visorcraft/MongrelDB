# mongreldb-node

Native Node.js addon for MongrelDB via [NAPI](https://napi.rs) — the
**better-sqlite3 model**: in-process, no HTTP latency, so the ~8 µs single-row
write isn't dwarfed by a network round-trip. Exposes a **typed object/method
interface** (not SQL); TypeScript types are generated at build time.

This crate is built **separately** from the Rust workspace (it targets the NAPI
ABI and needs Node.js tooling). It is excluded from `cargo {test,clippy}
--workspace`.

## Build

Requires Node.js 16+ and `@napi-rs/cli`:

```bash
cd crates/mongreldb-node
npm install
npm run build          # → mongreldb.<platform>.node + index.d.ts
```

## API sketch (generated `index.d.ts`)

```ts
class Database {
  constructor(path: string, schema: SchemaSpec, tableId: number)
  static open(path: string): Database
  put(cells: Cell[]): number            // returns rowId
  commit(): number                       // epoch
  flush(): number
  count(): number                        // O(1)
  get(rowId: number): Row | null
  query(conditions: ConditionSpec[]): Row[]   // hybrid: bitmap ∩ range ∩ FM ∩ HNSW
  close(): void
}
```

### Hybrid query — the differentiator

`query` intersects row-id sets from any combination of the six index types in a
single in-process call — something no HTTP vector DB or SQL FTS pipeline can do
in one hop:

```ts
db.query([
  { kind: ConditionKind.Ann, column_id: 5, embedding: queryVec, k: 50 },
  { kind: ConditionKind.FmContains, column_id: 2, text: "rome" },
  { kind: ConditionKind.BitmapEq, column_id: 3, text: "eu" },
])
```

## Notes

- Row ids / counts / epochs cross the FFI as JS `BigInt` (`u64`), so the full
  64-bit id space is lossless.
- Methods are synchronous and block the JS thread (the MongrelDB core is a
  synchronous `std::fs` store). Offloading hot loops to a worker thread is the
  remaining production polish.
