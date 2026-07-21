# Indexes

Indexes are how databases find data fast. Without an index, finding rows that
match a condition requires scanning every row. With an index, the database
jumps directly to the matching rows.

MongrelDB exposes six secondary index kinds: Bitmap, LearnedRange, FmIndex,
Ann, Sparse, and MinHash. Primary-key lookup and the PMA mutable-run tier are
internal and require no schema declaration.

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

## Index Types

### Primary Key Lookup

**What it does:** Instantly finds a row by its primary key value.

**How it works:** The primary-key lookup surface is implicit. Its current
in-memory implementation uses a `BTreeMap`.

**When to use:** Always - it's automatically built on whichever column you
mark `PRIMARY_KEY`.

**Example:**
```rust
// Uses HOT automatically
let q = Query::pk(42i64.to_be_bytes().to_vec());
let row = db.query(&q)?;
```

### Bitmap (Roaring) - Equality on Low-Cardinality Columns

**What it does:** Finds all rows where a column equals a specific value.

**How it works:** Each distinct value gets a compressed bitmap (a sorted set
of row IDs). To find rows where `status = 'active'`, look up the bitmap for
"active" and read the row IDs. Multiple conditions intersect cheaply (bitmap
AND operation).

**When to use:** Columns with a small number of distinct values - categories,
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
keys and unions those starting with the prefix - an exact lookup with no residual
re-check, tighter than `FmContains` for anchored matches.

```rust
IndexDef { name: "key_bm".into(), column_id: 2, kind: IndexKind::Bitmap }

// Query: all rows whose `key` (Bytes) starts with "user:".
Condition::BytesPrefix { column_id: 2, prefix: b"user:".to_vec() }
```

### PGM (Learned Index) - Range Queries

**What it does:** Finds all rows where a numeric column falls within a range.

**How it works:** Instead of a traditional B-tree, PGM uses a machine-learning
model (a piecewise linear approximation) to predict where values are located
in the sorted data. This is often smaller and faster than a B-tree for numeric
data.

**When to use:** Numeric columns that get range queries - timestamps, prices,
scores, IDs.

**Example:**
```rust
IndexDef { name: "price_pgm".into(), column_id: 3, kind: IndexKind::LearnedRange }

// Query
Condition::RangeF64 { column_id: 3, lo: 50.0, lo_inclusive: true, hi: 200.0, hi_inclusive: true }
```

### FM-index - Substring Search

**What it does:** Finds all rows where a text column contains a given substring.

**How it works:** Uses a Burrows-Wheeler Transform (BWT) and wavelet tree -
data structures from bioinformatics (they were invented for DNA sequencing).
Search time depends on the pattern length, not the data size.

**When to use:** Text columns where you need `LIKE '%keyword%'` search.
Regular B-tree indexes can't help with substring search - FM-index can.

**Example:**
```rust
IndexDef { name: "content_fm".into(), column_id: 4, kind: IndexKind::FmIndex }

// Query
Condition::FmContains { column_id: 4, pattern: b"database".to_vec() }
```

### ANN - Vector Similarity Search

**What it does:** Generates approximate nearest-neighbor candidates from an
embedding column. Algorithm and quantization are separate schema fields, with
only the combinations listed below implemented.

**Algorithms** (`algorithm = '…'` in `WITH (...)`):

| Algorithm | Structure | Best for |
|---|---|---|
| `hnsw` (default) | Multi-layer Hierarchical Navigable Small World graph | General-purpose, low-latency, in-memory |
| `diskann` | Single-layer Vamana robust-pruned graph (bounded degree R) | Large indexes, diverse-neighbor quality |
| `ivf` | Inverted file: k-means centroids + per-cell lists (probe nprobe) | Large indexes, tunable recall/speed tradeoff |

**Quantizations** (`quantization = '…'` in `WITH (...)`):

| Quantization | Stored representation | Distance (lower is better) | SQL/Arrow score field |
|---|---|---|---|
| `binary_sign` (default) | 1 bit per dimension | Hamming | `ann_distance: UInt32` |
| `dense` | full finite `f32` vectors | cosine distance `1 - cosine_similarity` | `ann_cosine_distance: Float32` |
| `product` | 8-bit PQ codes per subvector (trained codebook) | ADC (asymmetric); optional approximate reconstructed-vector rerank | `ann_distance: Float32` |

**Supported combinations:** `hnsw × {binary_sign, dense, product}`,
`diskann × dense`, `ivf × dense`. Other combinations are rejected at create
time (fail-closed) until their backends are wired.

Product quantization compresses vectors ~96× (a 768-dim f32 vector with 32
subvectors becomes 32 bytes) at the cost of approximate ADC distance. Setting
`pq_rerank_factor` reranks a bounded candidate set using reconstructed
approximate vectors. MongrelDB does not retain the original Dense vectors in
this backend, so this rerank is not exact.
Product currently uses a flat PQ scan. Its required `algorithm = 'hnsw'`
value is a compatibility selector and does not create an HNSW graph.

**How it works:**
- **HNSW** builds a multi-layer graph where similar vectors are connected by
  edges; search walks toward the query.
- **DiskANN** builds a single flat graph with robust-pruned R-degree neighbors
  and a fixed entry point; search is a greedy beam walk.
- **IVF** trains k-means centroids, assigns each vector to its nearest
  centroid's inverted list, and probes the nprobe nearest lists at query time.
- **Product quantization** trains per-subvector codebooks and encodes each
  vector to one byte per subvector; search computes an ADC lookup table from
  the query and sums table lookups per candidate.

Equal distances break ties by `RowId` in every mode. Dense and product indexes
use more memory and checkpoint space than BinarySign; product recovers most of
that gap via code compression.

**Online DDL:** `Database::create_index`, `replace_index`, and `drop_index`
(and SQL `CREATE INDEX` / `DROP INDEX`) build or remove a secondary index
generation without rewriting the table. Replacement (for example BinarySign →
Dense, or HNSW → DiskANN) is online except for a short final publication
barrier. Prefer `replace_index` over drop-then-create: schema validation allows
only one ANN representation per column. An algorithm or quantization change
never silently rewrites an existing index — it builds a hidden generation and
publishes atomically.

**When to use:** Embedding columns for AI/ML applications — semantic search,
recommendation, deduplication, clustering.

**Example:**
```rust
// ANN is built automatically from Value::Embedding data during put/bulk_load.
// Optional: create or replace with explicit algorithm + quantization.
// Database::replace_index("docs", "idx_embed", IndexDef {
//     options: IndexOptions { ann: Some(AnnOptions {
//         algorithm: AnnAlgorithm::DiskAnn,
//         quantization: AnnQuantization::Dense,
//         diskann: Some(DiskAnnOptions { r: 64, l: 128, beam_width: 8, alpha: 120 }),
//         ..AnnOptions::default()
//     }), ..IndexOptions::default() },
//     ..Default::default()
// })?;

// Query
Condition::Ann { column_id: 6, query: vec![0.1, 0.45, 0.78, ...], k: 10 }
```

```sql
-- HNSW + Dense (default algorithm)
CREATE INDEX idx_prompts_embed_ann
ON prompts USING ann (embedding)
WITH (quantization = 'dense', m = 16, ef_construction = 64, ef_search = 64);

-- DiskANN + Dense
CREATE INDEX idx_prompts_embed_diskann
ON prompts USING ann (embedding)
WITH (algorithm = 'diskann', quantization = 'dense',
      diskann_r = 64, diskann_l = 128, beam_width = 8, diskann_alpha = 120);

-- IVF + Dense
CREATE INDEX idx_prompts_embed_ivf
ON prompts USING ann (embedding)
WITH (algorithm = 'ivf', quantization = 'dense', nlist = 256, nprobe = 8);

-- Flat Product quantization (`hnsw` is the compatibility selector)
CREATE INDEX idx_prompts_embed_pq
ON prompts USING ann (embedding)
WITH (quantization = 'product', num_subvectors = 32, bits_per_subvector = 8,
      pq_training_samples = 256000, pq_seed = 42, pq_rerank_factor = 5);
```

### PMA Mutable-Run Tier

**What it does:** Maintains a sorted array that supports fast inserts without
the pointer-chasing of a B-tree.

**How it works:** A packed memory array keeps elements densely packed with
evenly spaced gaps. Inserts shuffle elements locally (O(log² n) amortized)
while maintaining cache-friendly sequential access.

**When to use:** Internal data structure for sorted runs - not directly
user-facing, but contributes to fast scan and merge performance.

### Sparse - SPLADE-style Sparse Retrieval

**What it does:** Ranks rows by sparse dot-product score against a query
sparse vector.

**How it works:** Stores each row's sparse vector (a list of token IDs with
weights) in an inverted index (token → list of rows containing it). At query
time, accumulates scores from matching tokens and returns the top-k.

**When to use:** Learned sparse retrieval - when you have SPLADE or similar
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

### MinHash - Set Similarity

**What it does:** Generates approximate candidates for rows whose member sets
have high Jaccard similarity.

**How it works:** Typed XXH3-64 member hashes feed MinHash signatures and LSH
buckets. Exact Jaccard verification can refine returned candidates but cannot
recover a missed LSH candidate.

**When to use:** Near-duplicate detection, set overlap, and similarity joins.

```rust
IndexDef { name: "members_mh".into(), column_id: 8, kind: IndexKind::MinHash }
```

## Choosing the Right Index

| Your query pattern | Recommended index |
|---|---|
| Fetch one row by ID | Primary-key lookup (automatic) |
| `WHERE category = 'X'` (few categories) | Bitmap |
| `WHERE price BETWEEN 10 AND 50` | PGM (LearnedRange) |
| `WHERE content LIKE '%keyword%'` | FM-index |
| Find similar items by embedding | HNSW |
| Rank documents by token relevance | Sparse |
| Find similar sets or duplicates | MinHash |
| Multiple equality filters | Multiple Bitmap indexes |

## Multiple Indexes Intersect

When you use multiple conditions in a query, each resolves independently to
a set of row IDs. These sets are then intersected (ANDed together). Only the
intersection - rows matching ALL conditions - gets decoded.

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
- `column IS NOT NULL` - index only non-null rows
- `column IS NULL` - index only null rows
- Unknown patterns conservatively index all rows.

`PRAGMA index_list(table)` shows `partial = 1` for indexes with a predicate.

## WITHOUT ROWID (Clustered Primary Key)

Tables created with `WITHOUT ROWID` use the primary key as the physical row
identity - sorted runs are logically keyed by PK rather than by a separate
monotonic `RowId`. This gives:

- **Idempotent upserts** - same PK always maps to the same row (no RowId
  allocation waste on repeated puts).
- **No hidden RowId** - the PK IS the row identity.

```sql
CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
```

The engine derives a deterministic `RowId` from the PK value (stable FNV-1a
hash) so the existing sorted-run and HOT-index infrastructure works unchanged.
