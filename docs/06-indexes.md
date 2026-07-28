# Indexes

Indexes are how databases find data fast. Without an index, finding rows that
match a condition requires scanning every row. With an index, the database
jumps directly to the matching rows.

MongrelDB exposes six secondary index kinds: Bitmap, LearnedRange, FmIndex,
Ann, Sparse, and MinHash. Primary-key lookup and the PMA mutable-run tier are
internal and require no schema declaration.

## Bitmap maintenance on update / PK replace

Product updates (`update_many`, and same-PK put that replaces a live row) keep
**Bitmap** secondaries consistent via a modular delta path
(`index::maintain`):

| Indexed value | Maintenance |
| --- | --- |
| **Unchanged** | **Repoint** — remove the old `RowId` under that key, insert the new `RowId` (no-op when the id is unchanged) |
| **Changed** | **Move** — remove the old key membership, insert under the new key |

That means a partial update that only touches non-indexed columns does **not**
leave the tombstoned row id in the Bitmap for the still-valid equality key.
ANN / FM / Sparse / MinHash / LearnedRange keep their existing full reindex
paths; Bitmap is the first family on the delta planner so equality/FK-style
indexes stay correct under high update volume.

### RangeInt point equality dual-sources Bitmap

Product clients (TypeScript Kit) push **int64 `eq()` as `RangeInt`**, not
`BitmapEq`. Range plans use LearnedRange / run scans + overlay. When `lo == hi`
and a Bitmap secondary exists on that column, the engine **also unions** the
Bitmap membership (then re-merges overlay). That way Bitmap maintenance on
update keeps product listing-by-FK / listing-by-owner correct even if a
run/LearnedRange plan would otherwise miss a live row.

### Pure deletes keep Bitmap membership (MVCC)

Pure deletes clear HOT but **do not** physically remove Bitmap memberships.
Historical pinned snapshots must still discover deleted row-ids via
`BitmapEq` while the pre-delete version is materializable. Count and
materialize paths filter tombstones via MVCC visibility (and
`had_deletes`-aware count materialization). Kit delete+put re-points Bitmap
keys on the subsequent put using a retained pre-image. Compaction /
`rebuild_indexes` rebuilds live memberships and re-indexes pin-needed
historical discovery keys when pins are active.

### Rebuild after desync

`Database::rebuild_indexes(table)` / `rebuild_all_indexes()` reconstruct HOT
and every secondary from runs + mutable run + memtable. Use when a fullscan
still finds a row that PK or equality listing misses.

Primary-key lookup also **falls back** to a targeted PK-column scan when the
HOT map misses a key that may still be live (self-heal for rare desync).

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

Dense and product indexes use more memory and checkpoint space than BinarySign;
product recovers most of that gap via code compression.

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
-- HNSW + Dense (recommended production quality path; see
-- docs/24-production-single-node-hnsw-auth.md)
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

## Exact vs approximate guarantees

Exact families must either complete or return an explicit work-budget error;
they never silently degrade to approximate results. Approximate floors below
are recall-at-k guarantees on the deterministic oracle corpus and documented
configuration, not a claim that every production corpus has identical recall.

| Family / mode | Guarantee | Tie-break rule | Candidate-cap behavior | Work-budget behavior | Recall floor |
|---|---|---|---|---|---|
| Bitmap | Exact equality or anchored prefix | `RowId` | No approximate candidate cap; never silently truncates | Must complete or return a budget error | Exact; no floor |
| LearnedRange | Exact range | `RowId` | No approximate candidate cap; never silently truncates | Must complete or return a budget error | Exact; no floor |
| FmIndex | Exact substring | `RowId` | No approximate candidate cap; never silently truncates | Must complete or return a budget error | Exact; no floor |
| Sparse | Exact dot-product top-k | Higher score, then `RowId` | No approximate candidate cap; never silently truncates | Must complete or return a budget error | Exact; no floor |
| MinHash | Approximate LSH candidates; exact verification cannot recover missed candidates | Higher verified Jaccard score, then `RowId` | May truncate candidates; cap hit must be traced | Bounded search; exhaustion must be explicit | 0.80 (see below) |
| ANN HNSW BinarySign | Approximate | Smaller distance, then `RowId` | May truncate candidates; cap hit must be traced | Bounded search; exhaustion must be explicit | 0.95 |
| ANN HNSW Dense | Approximate | Smaller distance, then `RowId` | May truncate candidates; cap hit must be traced | Bounded search; exhaustion must be explicit | 0.90 |
| ANN DiskANN Dense | Approximate | Smaller distance, then `RowId` | May truncate candidates; cap hit must be traced | Bounded search; exhaustion must be explicit | 0.90 |
| ANN IVF Dense | Approximate | Smaller distance, then `RowId` | May truncate candidates; cap hit must be traced | Bounded search; exhaustion must be explicit | 0.85 |
| ANN Product Quantization | Approximate, including reconstructed-vector rerank | Smaller distance, then `RowId` | Rerank candidates may be capped; cap hit must be traced | Bounded scan and rerank; exhaustion must be explicit | 0.80 with rerank |

### Per-family recall floors

- **FmIndex:** exact substring matching; no recall floor.
- **LearnedRange:** exact range matching; no recall floor.
- **Sparse:** exact dot-product ranking; the churn oracle asserts exact
  top-k membership, exact `RowId` ordering, and score equality within 1e-5
  against an independent model at every checkpoint.
- **MinHash:** 0.80 median recall on the deterministic oracle corpus (see
  below); the exact-duplicate gate is 1.0.
- **ANN HNSW BinarySign:** 0.95.
- **ANN HNSW Dense:** 0.90.
- **ANN DiskANN Dense:** 0.90.
- **ANN IVF Dense:** 0.85.
- **ANN Product Quantization:** 0.80 with rerank.

### MinHash recall floor

`MINHASH_GENERAL_RECALL_FLOOR` (in
`crates/mongreldb-core/tests/index_churn_oracle.rs`) is **0.80**. It is
enforced as the *median* tie-tolerant recall across every checkpoint of a
500-operation churn run, with an additional per-checkpoint guard that
recall never collapses to zero on a non-empty oracle answer. Measured
healthy recall on the deterministic corpus (128-permutation signatures,
32 LSH bands, near-duplicate-heavy corpus, 500 operations per seed) is a
**median of 1.0 on every seed of the CI matrix (1-8)**; individual
checkpoints occasionally dip (observed min samples 0.33-1.0) when an LSH
band miss or an estimator reordering drops one top-k row, which is why
the gate is a median rather than a per-checkpoint minimum. The floor sits
far below the healthy median but far above total failure. Lowering the
floor — ever below 0.80 — requires an approved ADR.

Two complementary gates apply:

- **Exact-duplicate gate (recall 1.0):** at least k live rows whose stored
  set equals the query set are always returned with estimated Jaccard 1.0,
  in ascending-`RowId` tie-break order, with no stale/deleted/expired row.
- **General corpus gate (floor 0.80):** recall is tie/estimation-noise
  tolerant — an expected top-k row counts as found when the engine returns
  it or substitutes a row whose exact Jaccard is at least as high. A
  genuine LSH band miss can only substitute lower-similarity rows and is
  counted as a miss.

Sparse and MinHash retrieval honor the caller-supplied snapshot through
`Table::retrieve_at`: a pinned snapshot answers with the historical
(postings-visible) state across update, delete, flush, compaction, and
close+reopen; the current snapshot reflects the latest committed state.

### Tie-break rules

Equal distances break ties by `RowId` in every mode. ANN ranks smaller distance
first, then `RowId`. Sparse ranks larger dot product first, then `RowId`, and
MinHash ranks larger verified Jaccard similarity first, then `RowId`.

### Candidate caps

When a candidate cap is reached, the query trace must report
`candidate_cap_hit=true`. A result containing fewer than the requested `k`
rows must include an explicit underfill reason; candidate-cap or work-budget
exhaustion must never look like a complete result.

### Recall floor enforcement

The `index-churn-oracle-smoke` CI job must verify every documented floor on
every pull request. The nightly oracle job must verify the same floors on a
fresh corpus.

### `MONGRELDB_ORACLE_SEED`

Set `MONGRELDB_ORACLE_SEED` to the integer seed printed by a failed oracle run
to replay its deterministic corpus, queries, and churn sequence. Replay also
requires the same binary and oracle configuration. Leave it unset for a fresh
corpus; every run must print its selected seed so a failure can be reproduced.

### Churn-oracle coverage axes (nightly/weekly workflows)

`crates/mongreldb-core/tests/index_churn_oracle.rs` honors the coverage-axis
env contract consumed by `.github/workflows/index-churn-nightly.yml` and
`index-churn-weekly.yml` (full contract in `scripts/churn-history-check.sh`).
With none of these set, the harness runs the historical PR-smoke mix
byte-for-byte; every knob only adds or removes coverage. `"1"` enables an
axis' enhanced behavior, `"0"` removes the axis from the op mix entirely —
including when the weekly profile would otherwise imply it:

- `MONGRELDB_ORACLE_ENCRYPTION`: `"1"` creates the churn table itself with
  AES-256-GCM (`Table::create_encrypted`, reopened via `open_encrypted`);
  `"0"` also removes the encrypted-sibling lifecycle ops from the mix.
- `MONGRELDB_ORACLE_TTL`: `"1"` adds an expire-everything TTL op (a 1µs TTL,
  deterministic because the timestamp column always sits more than 1µs in
  the past by the next checkpoint); `"0"` removes the set/clear TTL ops.
- `MONGRELDB_ORACLE_HISTORICAL_SNAPSHOTS`: `"1"` adds pinned historical
  snapshot reads — the harness captures the engine's visible row set at pin
  time and asserts a later re-read of the pinned epoch never gains rows
  (TTL expiry may only shrink it; a TTL policy change forces a re-pin).
  `"0"` removes the snapshot-pin ops.
- `MONGRELDB_ORACLE_CANDIDATE_CAP_PRESSURE`: `"1"` adds a probe retrieval
  with `max_fused_candidates = 1` so ANN queries exceed the candidate cap;
  the trace's `candidate_cap_hit` is counted into the metrics JSON. The
  hard candidate cap binds on the ANN retrieval path only: Sparse and
  MinHash still run the probe (their paths accept the constrained
  execution context) and FM/LearnedRange keep the delete-based pressure
  op, because the exact query path exposes no candidate cap.
- `MONGRELDB_ORACLE_WORK_BUDGET_PRESSURE`: `"1"` adds a work-budget probe:
  a zero-budget retrieval must fail explicitly with `WorkBudgetExceeded`
  (or charge nothing on an empty index), and a generous budget must
  succeed. Only the scored-retrieval surface (`Table::retrieve…`) accepts
  a work budget, so the probe is engine-inert for FM/LearnedRange — an
  honest limitation, not a silent skip.
- `MONGRELDB_ORACLE_LIFECYCLE_OPS`: `"1"` raises flush/compact/rebuild/
  close+reopen to full op-matrix weight; `"0"` removes all lifecycle ops.
- `MONGRELDB_ORACLE_WEEKLY_PROFILE=1` implies `"1"` for every axis above
  except encryption (the weekly workflow sets encryption per seed), and
  tunes the mix: hot-key churn biased by
  `MONGRELDB_ORACLE_STALE_CANDIDATE_RATIO` (default 100 — a target bias,
  not a guaranteed ratio; the achieved stale:live ratio is reported in the
  metrics JSON), `MONGRELDB_ORACLE_HOT_KEY_HISTORY` (default 512 target
  versions per hot key), plus explicit, step-scheduled compaction
  (`MONGRELDB_ORACLE_COMPACTION_CYCLES`, default 8) and close+reopen
  (`MONGRELDB_ORACLE_REOPEN_CYCLES`, default 4) cycles.
- `MONGRELDB_ORACLE_METRICS_JSON`: path to a JSON object keyed by record
  name with per-family op counts, op/query wall-clock latency percentiles
  (p50/p95/p99/max), cap-hit and budget-trip counts, achieved stale:live
  ratio, and peak RSS (`VmHWM`). Family tests share one process, so each
  family merges its entry under a lock. The weekly workflow parses
  `max_rss_kb` from `/usr/bin/time -v` externally.
- `MONGRELDB_ORACLE_FAILURE_DIR`: on any oracle assertion failure the
  harness copies the failing database directory, the full operation log,
  and the panic context into `<dir>/<family>-seed-<seed>/` before
  re-raising the panic.

Known oracle limitation: compaction physically reclaims TTL-expired rows,
while the oracle model treats TTL as a query-time filter that `clear_ttl`
reverses. The harness therefore asserts TTL-adjacent properties
directionally (the auth allowed-set check is subset-based: the engine must
never return a row outside the allowed set, but a legitimately reclaimed
row may be absent), matching the long-standing soft final-consistency
check.

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
