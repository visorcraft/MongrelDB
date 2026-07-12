# Native Queries (Condition API)

The Condition API is MongrelDB's signature feature. Instead of SQL strings,
you build queries by composing typed conditions - each backed by a specific
index. This lets you combine search modes that no SQL engine can express in
a single query.

For example, you can find rows that **contain text "rust"** AND **are within
a price range** AND **are semantically similar to a vector** - all in one
call, with each condition resolved by its own index.

## Building a Query

Start with `Query::new()` and chain `.and()` calls:

```rust
use mongreldb_core::query::{Condition, Query};

let q = Query::new()
    .and(Condition::BitmapEq {
        column_id: 2,
        value: b"premium".to_vec(),
    })
    .and(Condition::RangeF64 {
        column_id: 3,
        lo: 50.0,
        lo_inclusive: true,
        hi: 200.0,
        hi_inclusive: true,
    });

let results = db.query(&q).unwrap();
```

All conditions are ANDed together - a row must match every condition to be
in the result set.

## Condition Types

### Primary Key Lookup

```rust
Condition::Pk(42i64.to_be_bytes().to_vec())
```

Finds the single row with primary key 42. Uses the HOT trie index - O(log n)
point lookup. This is the fastest way to fetch one row.

### Bitmap Equality

```rust
Condition::BitmapEq {
    column_id: 2,
    value: b"premium".to_vec(),
}
```

Finds all rows where column 2 equals "premium". Uses a Roaring bitmap - each
distinct value maps to a compressed bitmap of row IDs. Best for columns with
a small number of distinct values (categories, statuses, regions).

### IN-List (Multiple Values)

```rust
Condition::BitmapIn {
    column_id: 2,
    values: vec![b"gold".to_vec(), b"silver".to_vec(), b"bronze".to_vec()],
}
```

Finds rows where column 2 is any of the listed values. Internally unions
multiple bitmap lookups.

### Range (Integer)

```rust
Condition::Range {
    column_id: 5,
    lo: 1700000000,
    hi: 1700001000,
}
```

Finds rows where column 5 is between 1700000000 and 1700001000 (inclusive).
Uses the PGM learned index - a machine-learning model that predicts where
values are located in the sorted data, giving sub-linear lookup time.

### Range (Float)

```rust
Condition::RangeF64 {
    column_id: 3,
    lo: 50.0,
    lo_inclusive: true,
    hi: 200.0,
    hi_inclusive: false,  // hi is exclusive
}
```

Same as integer range but for `Float64` columns. Per-bound inclusivity lets
you express `>`, `>=`, `<`, `<=`, and `BETWEEN` precisely.

### Substring Search (FM-index)

```rust
Condition::FmContains {
    column_id: 4,
    pattern: b"database".to_vec(),
}
```

Finds rows where column 4 contains the text "database" anywhere. Uses a
Burrows-Wheeler Transform + wavelet tree - the same family of techniques
used by full-text search engines. Searches in time proportional to the
pattern length, not the data size.

### Vector Similarity (HNSW)

```rust
Condition::Ann {
    column_id: 6,
    query: vec![0.1, 0.45, 0.78, 0.23, ...],  // same dimension as your embeddings
    k: 10,  // return top 10 nearest neighbors
}
```

Finds the 10 rows whose embedding vector (stored in column 6) is closest to
the query vector. Uses HNSW (Hierarchical Navigable Small World) - a graph
index that gives approximate nearest neighbor search with recall@10 ≥ 90%.

### Sparse Retrieval (SPLADE-style)

```rust
Condition::SparseMatch {
    column_id: 7,
    query: vec![(42, 1.5), (108, 0.8), (256, 2.1)],  // token_id → weight
    k: 10,
}
```

Finds the top 10 rows by sparse dot-product score. The query is a sparse
vector (a list of token IDs with weights), and each row's column 7 stores
a sparse vector in the same format. This is the retrieval model used by
SPLADE and other learned sparse retrievers.

### Anchored Prefix on Bytes (Bitmap)

```rust
Condition::BytesPrefix {
    column_id: 2,
    prefix: b"user:".to_vec(),
}
```

Finds rows where column 2 (a `Bytes` column with a bitmap index) starts with the
bytes `user:`. This is the exact equivalent of SQL `LIKE 'prefix%'` (no
wildcards in `prefix`), but resolves **exactly** - the bitmap's distinct keys are
enumerated and filtered by prefix, with no residual re-check. Tighter and faster
than `FmContains` for anchored matches on indexed Bytes columns. Returns an empty
set if the column has no bitmap index.

### Null checks

```rust
Condition::IsNull { column_id: 3 }      // column 3 IS NULL
Condition::IsNotNull { column_id: 3 }   // column 3 IS NOT NULL
```

Page-stat pruned: pages whose statistics show all-non-null (for `IsNull`) or
all-null (for `IsNotNull`) are skipped entirely.

### Multi-segment substring (FM-index)

```rust
Condition::FmContainsAll {
    column_id: 4,
    patterns: vec![b"hello".to_vec(), b"world".to_vec()],
}
```

Like `FmContains` but intersects multiple substrings (`LIKE '%a%b%c%'`). Returns
a superset - the caller re-checks order/wildcards if needed.

### Set similarity (MinHash / LSH)

```rust
Condition::MinHashSimilar {
    column_id: 8,
    query: vec![0x1234, 0x5678, 0xabcd],  // hashed query-set members
    k: 10,
}
```

The column stores a JSON array of set members. MongrelDB hashes the members
and builds an internal MinHash signature; queries provide the corresponding
set members or hashes.

## Node.js (NAPI addon)

The same condition set is available from JavaScript via `ConditionSpec` and the
`ConditionKind` enum (TypeScript types ship in `native.d.ts`). Build a query as
an array of `ConditionSpec` objects and pass it to `table.query(conditions)` (or
`queryArrow` for Arrow IPC bytes):

```javascript
import { ConditionKind } from '@visorcraft/mongreldb';

const table = db.table('events');

// Bitmap equality on a low-cardinality column.
const active = table.query([
  { kind: ConditionKind.BitmapEq, columnId: 3, text: 'active' },
]);

// Anchored prefix on a bitmap-indexed Bytes column (exact pushdown).
const userEvents = table.query([
  { kind: ConditionKind.BytesPrefix, columnId: 2, text: 'user:' },
]);

// Hybrid: combine an ANN search with a bitmap filter in one intersection.
const relevant = table.query([
  { kind: ConditionKind.Ann, columnId: 6, embedding: queryVector, k: 10 },
  { kind: ConditionKind.BitmapEq, columnId: 3, text: 'published' },
]);

// Arrow IPC bytes (zero-copy columnar) instead of JS objects.
const ipc = table.queryArrow([
  { kind: ConditionKind.RangeInt, columnId: 1, int64Lo: 100n, int64Hi: 9223372036854775807n },
]);
```

`ConditionSpec` fields: `kind`, `columnId`, `int64Lo`/`int64Hi` (BigInt),
`float64Lo`/`float64Hi`, `text` (used by `BitmapEq`, `FmContains`,
`BytesPrefix`), `values` (`BitmapIn`, `MinHashSimilar`), `embedding` + `k`
(`Ann`), `sparseTokens` + `sparseWeights` (`SparseMatch`).

## Combining Conditions

This is where the Condition API shines. You can mix any conditions on any
columns - they all resolve through the shared RowId space:

```rust
// "Find premium users, in a price range, whose profile mentions 'rust',
//  and whose embedding is close to this vector - top 5 by similarity"
let q = Query::new()
    .and(Condition::BitmapEq {
        column_id: 2,
        value: b"premium".to_vec(),
    })
    .and(Condition::RangeF64 {
        column_id: 3,
        lo: 50.0, lo_inclusive: true,
        hi: 200.0, hi_inclusive: true,
    })
    .and(Condition::FmContains {
        column_id: 4,
        pattern: b"rust".to_vec(),
    })
    .and(Condition::Ann {
        column_id: 6,
        query: my_embedding.clone(),
        k: 5,
    });

let results = db.query(&q).unwrap();
```

Each condition independently resolves to a set of row IDs, then they're
intersected. Only the survivors are materialized (decoded from columnar
storage into row objects).

## Projection

By default, `query()` returns all columns. To fetch only specific columns
(faster - less decoding):

```rust
let snap = db.snapshot();
let cols = db.query_columns_native(
    &[condition],
    Some(&[1, 3]),  // only columns 1 and 3
    snap,
).unwrap();
```

This returns `NativeColumn` typed buffers instead of `Row` objects - even
faster for bulk processing.

## Cached Queries

```rust
let results = db.query_cached(&q).unwrap();
```

Same as `query()` but checks the result cache first. On a cache hit, returns
the pre-computed result in ~0.1 µs. The cache is invalidated intelligently
when committed changes affect the query's footprint.

## When to Use Conditions vs SQL

| Situation | Use |
|---|---|
| Standard SELECT/WHERE | SQL (either works) |
| Composing 3+ different index types in one query | Conditions |
| Vector similarity (ANN) | Conditions or SQL (`ann_search` UDF) |
| Application code building dynamic filters | Conditions |
| Ad-hoc analysis / BI tools | SQL |
| Need the absolute fastest point lookup | `Condition::Pk` |
