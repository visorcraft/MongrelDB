# ADR-0009: AI Index Replication and Rebuild Policy

- Status: Accepted
- Date: 2026-07-17
- Spec references: §9.1 (FND-001), §4.8 (derived-index invariant), §13.3
  (Stage 4C AI index generations), §10.3 (Stage 1C immutable read/index
  generations), §10.6 (build-and-publish), §13.4/§13.6 (distributed retrieval
  and consistency metadata)

## Context

MongrelDB has six public secondary index kinds — `Bitmap`, `FmIndex`, `Ann`,
`LearnedRange`, `MinHash`, `Sparse` (`crates/mongreldb-core/src/schema.rs:233`,
`IndexKind`; implementations under `crates/mongreldb-core/src/index/`). The ANN
kind is HNSW-based (`index/hnsw.rs`) with quantized vectors; `Sparse` is a
SPLADE-style learned-sparse retrieval index; `MinHash` is LSH set similarity.

In a replicated cluster, the internal bytes of these structures are not a
function of the input data alone: HNSW graph shape depends on randomized level
assignment and insertion order, quantization codebooks on training runs, and
roaring/FM/PGM layouts on build parameters and library versions. Requiring
byte-identical structures across replicas would force serialized deterministic
builds, pin every replica to identical binaries and SIMD paths, and make
rolling upgrades of index code impossible.

Meanwhile the invariants are explicit (§4.8): ANN, Sparse, MinHash, FM,
bitmap, and range indexes are *derived* from replicated row data and index
definitions; loss of an index may reduce performance or approximate recall but
cannot lose authoritative rows; index generations are versioned and atomically
published. And because every read has an explicit snapshot timestamp (§4.5),
an index may only answer a read at timestamp T if it has applied every
committed change through T.

## Decision

1. **Derived, local, never authoritative.** AI/secondary indexes are per-node
   derived state. They are never a durability authority: the replicated,
   authoritative inputs are

   - row data
   - embedding/sparse/set values
   - index definitions
   - preprocessing/model version metadata

   (§13.3 replication policy). Row data includes the raw vector, sparse-weight,
   and set column values indexes are built from, so any replica can rebuild
   any index from the committed log alone.

2. **No byte-identical internal structures.** We do not replicate, compare, or
   require byte-identical HNSW graphs — or any other index-internal bytes —
   across replicas (§13.3). Each replica builds and maintains its own derived
   index generations locally from the committed log.

3. **Versioned generations, atomically published.** Every index generation
   records, per §13.3 (`AiIndexGeneration`): `index_id`,
   `definition_version`, `applied_through` (the HLC timestamp of the committed
   log position the generation covers), `source_schema_version`,
   `preprocessing_version`, `model_version`, `base_generation`, and
   `delta_generations`. Generations are published atomically (§4.8, §10.3
   S1C-002): readers pin a generation; a replacement becomes visible in one
   atomic publish; old generations are reclaimed only after all pins drop
   (§10.3 S1C-004).

4. **Base plus delta structure.** Each index family maintains an immutable
   base, zero or more immutable frozen deltas, one active mutable delta, and a
   visibility/tombstone filter (§10.3 S1C-003). For ANN specifically: base
   HNSW + small delta HNSW + candidate merge + exact rerank. Compaction merges
   deltas into a new base. Rebuilds and schema-driven changes follow
   build-and-publish (§10.6 S1F-003): record pending definition, pin a
   snapshot, build a hidden generation, catch up committed deltas, validate,
   publish atomically, release the old generation after pins drop.

5. **Readiness rule.** A replica may serve an indexed read only if

   ```text
   index.applied_through >= requested read timestamp
   ```

   Otherwise it must do one of the following, per the request's declared
   consistency policy (§13.3, §13.6):

   - wait (bounded by the request deadline),
   - route the read to a replica whose index is ready,
   - use the documented exact fallback (scan/brute-force over authoritative
     row data at the read timestamp), or
   - return `IndexNotReady`.

   Serving indexed results that miss committed rows visible at the request's
   snapshot timestamp is never allowed silently.

6. **Preprocessing and model versions are part of the definition.** Changing
   `preprocessing_version` or `model_version` creates a new
   `definition_version` and schedules a rebuild through the same
   build-and-publish online job. Responses that used an AI index return the
   read timestamp, replica applied timestamp, staleness, index applied
   timestamp, and model/preprocessing version (§13.6) so results are
   auditable. Mixed-version serving is permitted only when the request opts
   into it and the versions are reported.

## Alternatives Considered

1. **Replicate byte-identical index images** (ship built HNSW/FM/bitmap bytes
   through the log or snapshots). Rejected: forbids independent implementation
   changes and rolling upgrades, amplifies log and snapshot bandwidth, and
   couples correctness to deterministic builds of randomized structures.
   §13.3 explicitly does not require byte-identical HNSW graphs.

2. **Leader builds; followers install index snapshots.** Rejected: follower
   catch-up stalls on large artifact installs, bootstrap of a new replica
   blocks on derived bytes instead of just rows, and index bytes become
   quasi-authoritative — blurring the §4.8 derived/authoritative boundary.

3. **Approximate-anytime serving without `applied_through` tracking.**
   Rejected: results could omit rows committed before the request's snapshot
   timestamp, violating MVCC visibility (§4.5) and making RAG/search results
   non-auditable (§13.6).

4. **Block reads until the newest index generation is ready.** Rejected:
   unbounded latency during rebuilds. The readiness policy offers explicit,
   documented choices (wait / route / exact fallback / `IndexNotReady`)
   instead of a hidden stall or a silent stale answer.

## Consequences

Positive:

- Replication bandwidth carries only logical data and definitions; snapshots
   and catch-up stay small.
- Replicas may run different builds, quantization, SIMD paths, and library
   versions — compatible with rolling upgrades (ADR-0010).
- Index corruption or loss is a local rebuild from replicated inputs, never
   data loss; a new replica becomes fully index-capable from row data alone.
- Readiness is explicit and auditable; exact fallback keeps correctness
   independent of index freshness.

Negative / costs:

- Every replica pays CPU/memory to build and maintain its own indexes; cost
   scales with replica count.
- Approximate recall can differ across replicas during catch-up; where
   determinism is required, §13.4 defines tie-breaks (final score desc,
   tablet id asc, `RowId` asc) and optional exact rerank.
- Every indexed read path must carry and check `applied_through` against the
   read timestamp; the wait/route/fallback/`IndexNotReady` policy must be
   plumbed through the request consistency declaration.
- Model/preprocessing upgrades are full rebuilds scheduled as online jobs;
   they must be planned operationally like index builds.

## Migration

1. **Stage 1C (§10.3, single node):** introduce `IndexGeneration` with
   per-family generations and `applied_through`; atomic publish; base +
   delta + tombstone filter. Existing on-disk indexes become the first base
   generation with `applied_through` initialized from the table's
   `visible_through`; no user data migration is required.
2. **Stage 1F (§10.6):** index builds and rebuilds become online schema jobs
   using build-and-publish; definition changes record new definition
   versions in the catalog (ADR-0008).
3. **Stages 2–3:** replicas derive indexes from the committed log; the
   readiness rule is enforced on every indexed read; snapshot/catch-up
   carries rows and definitions only.
4. **Stage 4 (§13.3–§13.6):** `AiIndexGeneration` metadata, per-request
   consistency declarations, and audit metadata become part of the protocol;
   distributed retrieval (§13.4) consumes per-replica generations.

## Reversal Strategy

- Indexes are droppable and rebuildable at any time: reversing any index
  generation or build change is "unpublish/drop the generation, rebuild from
  row data." No durable user data is involved, so reversal can never lose
  authoritative rows (§4.8).
- If the local-build policy itself were ever revisited (e.g. toward shipped
  index images), the already-replicated inputs — rows, embedding/sparse/set
  values, definitions, preprocessing/model versions — are everything a build
  needs; moving to image shipping would be an additive change to the catch-up
  path, not a destructive migration.
- Rolling back index *code* is covered by ADR-0010: because internal index
  bytes are never exchanged, mixed index-code versions coexist safely as long
  as definitions and readiness metadata remain version-compatible.
