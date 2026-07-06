# Indexes

Indexes are how databases find data fast. Without an index, finding rows that
match a condition requires scanning every row. With an index, the database
jumps directly to the matching rows.

MongrelDB has eight index types — each designed for a different kind of query.
Most databases have one or two index types. Having eight means MongrelDB can
accelerate search patterns that other databases can't (like semantic vector
search or substring search).

## How to Declare Indexes

Indexes are defined in the schema when you create a table:

```rust
let schema = Schema {
    schema_id: 1,
    columns: vec![ /* ... */ ],
    indexes: vec![
        IndexDef { name: "status_bm".into(), column_id: 3, kind: IndexKind::Bitmap },
        IndexDef { name: "ts_range".into(), column_id: 5, kind: IndexKind::LearnedRange },
        IndexDef { name: "content_fm".into(), column_id: 4, kind: IndexKind::FmIndex },
    ],
    colocation: vec![],
};
```

You can also add a PGM range index after data is loaded:

```rust
db.add_learned_range_index("timestamp")?;
```

## The Eight Index Types

### 1. HOT (Height-Optimized Trie) — Primary Key Lookup

**What it does:** Instantly finds a row by its primary key value.

**How it works:** A trie (prefix tree) that's been flattened to minimize
height. Looking up a key walks the trie structure — O(key length), not
O(number of rows).

**When to use:** Always — it's automatically built on whichever column you
mark `PRIMARY_KEY`.

**Example:**
```rust
// Uses HOT automatically
let q = Query::pk(42i64.to_be_bytes().to_vec());
let row = db.query(&q)?;
```

### 2. Bitmap (Roaring) — Equality on Low-Cardinality Columns

**What it does:** Finds all rows where a column equals a specific value.

**How it works:** Each distinct value gets a compressed bitmap (a sorted set
of row IDs). To find rows where `status = 'active'`, look up the bitmap for
"active" and read the row IDs. Multiple conditions intersect cheaply (bitmap
AND operation).

**When to use:** Columns with a small number of distinct values — categories,
statuses, regions, booleans. If your column has fewer than ~10,000 distinct
values, bitmap is a good choice.

**Example:**
```rust
IndexDef { name: "status_bm".into(), column_id: 3, kind: IndexKind::Bitmap }

// Query
Condition::BitmapEq { column_id: 3, value: b"active".to_vec() }
```

A bitmap index on a `Bytes` column also accelerates **anchored prefix matching**
(`LIKE 'prefix%'`): `Condition::BytesPrefix` enumerates the bitmap's distinct
keys and unions those starting with the prefix — an exact lookup with no residual
re-check, tighter than `FmContains` for anchored matches.

```rust
IndexDef { name: "key_bm".into(), column_id: 2, kind: IndexKind::Bitmap }

// Query: all rows whose `key` (Bytes) starts with "user:".
Condition::BytesPrefix { column_id: 2, prefix: b"user:".to_vec() }
```

### 3. PGM (Learned Index) — Range Queries

**What it does:** Finds all rows where a numeric column falls within a range.

**How it works:** Instead of a traditional B-tree, PGM uses a machine-learning
model (a piecewise linear approximation) to predict where values are located
in the sorted data. This is often smaller and faster than a B-tree for numeric
data.

**When to use:** Numeric columns that get range queries — timestamps, prices,
scores, IDs.

**Example:**
```rust
IndexDef { name: "price_pgm".into(), column_id: 3, kind: IndexKind::LearnedRange }

// Query
Condition::RangeF64 { column_id: 3, lo: 50.0, lo_inclusive: true, hi: 200.0, hi_inclusive: true }
```

### 4. FM-index — Substring Search

**What it does:** Finds all rows where a text column contains a given substring.

**How it works:** Uses a Burrows-Wheeler Transform (BWT) and wavelet tree —
data structures from bioinformatics (they were invented for DNA sequencing).
Search time depends on the pattern length, not the data size.

**When to use:** Text columns where you need `LIKE '%keyword%'` search.
Regular B-tree indexes can't help with substring search — FM-index can.

**Example:**
```rust
IndexDef { name: "content_fm".into(), column_id: 4, kind: IndexKind::FmIndex }

// Query
Condition::FmContains { column_id: 4, pattern: b"database".to_vec() }
```

### 5. HNSW — Vector Similarity Search

**What it does:** Finds the k rows whose embedding vector is closest (by
Euclidean or cosine distance) to a query vector.

**How it works:** Builds a multi-layer graph where similar vectors are
connected by edges. Search walks the graph from a random entry point,
greedily moving toward the query. Achieves recall@10 ≥ 90% with sub-linear
time.

**When to use:** Embedding columns for AI/ML applications — semantic search,
recommendation, deduplication, clustering.

**Example:**
```rust
// HNSW is built automatically from Value::Embedding data during put/bulk_load

// Query
Condition::Ann { column_id: 6, query: vec![0.1, 0.45, 0.78, ...], k: 10 }
```

### 6. PMA (Packed Memory Array) — Cache-Oblivious Sorted Runs

**What it does:** Maintains a sorted array that supports fast inserts without
the pointer-chasing of a B-tree.

**How it works:** A packed memory array keeps elements densely packed with
evenly spaced gaps. Inserts shuffle elements locally (O(log² n) amortized)
while maintaining cache-friendly sequential access.

**When to use:** Internal data structure for sorted runs — not directly
user-facing, but contributes to fast scan and merge performance.

### 7. Sparse — SPLADE-style Sparse Retrieval

**What it does:** Ranks rows by sparse dot-product score against a query
sparse vector.

**How it works:** Stores each row's sparse vector (a list of token IDs with
weights) in an inverted index (token → list of rows containing it). At query
time, accumulates scores from matching tokens and returns the top-k.

**When to use:** Learned sparse retrieval — when you have SPLADE or similar
sparse vector representations of text and want relevance ranking.

**Example:**
```rust
// Store sparse vectors as bincode-serialized Vec<(u32, f32)> in a Bytes column
IndexDef { name: "sparse_idx".into(), column_id: 7, kind: IndexKind::Sparse }

// Query
Condition::SparseMatch {
    column_id: 7,
    query: vec![(42, 1.5), (108, 0.8), (256, 2.1)],
    k: 10,
}
```

## Choosing the Right Index

| Your query pattern | Recommended index |
|---|---|
| Fetch one row by ID | HOT (automatic on PK) |
| `WHERE category = 'X'` (few categories) | Bitmap |
| `WHERE price BETWEEN 10 AND 50` | PGM (LearnedRange) |
| `WHERE content LIKE '%keyword%'` | FM-index |
| Find similar items by embedding | HNSW |
| Rank documents by token relevance | Sparse |
| Multiple equality filters | Multiple Bitmap indexes |

## Multiple Indexes Intersect

When you use multiple conditions in a query, each resolves independently to
a set of row IDs. These sets are then intersected (ANDed together). Only the
intersection — rows matching ALL conditions — gets decoded.

This means adding more indexes makes multi-condition queries faster, not
slower. Each index narrows the result set before any data is scanned.

## Partial Indexes

A partial index covers only rows matching a `WHERE` predicate. This is useful
for large tables where queries typically filter on a condition (e.g. only
active records):

```sql
-- Index only non-deleted rows
CREATE INDEX idx_active_users ON users (email) WHERE deleted_at IS NULL;
```

The predicate is stored on `IndexDef` and evaluated at index-build time. Rows
not matching the predicate are skipped. Supported predicate patterns:
- `column IS NOT NULL` — index only non-null rows
- `column IS NULL` — index only null rows
- Unknown patterns conservatively index all rows.

`PRAGMA index_list(table)` shows `partial = 1` for indexes with a predicate.

## WITHOUT ROWID (Clustered Primary Key)

Tables created with `WITHOUT ROWID` use the primary key as the physical row
identity — sorted runs are logically keyed by PK rather than by a separate
monotonic `RowId`. This gives:

- **Idempotent upserts** — same PK always maps to the same row (no RowId
  allocation waste on repeated puts).
- **No hidden RowId** — the PK IS the row identity.

```sql
CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
```

The engine derives a deterministic `RowId` from the PK value (stable FNV-1a
hash) so the existing sorted-run and HOT-index infrastructure works unchanged.
