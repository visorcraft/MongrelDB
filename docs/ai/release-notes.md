# AI retrieval compatibility notes

- Kit table creation now accepts typed index definitions and typed ANN, Sparse,
  and set values. Malformed AI values return HTTP 400 instead of becoming NULL.
- MinHash raw members use the documented typed XXH3-64 v1 contract.
- Pre-hashed `u64` MinHash queries are an advanced, version-sensitive API.
  Clients that produced old `DefaultHasher` values must send raw members or
  migrate to v1 hashes.
- Global index checkpoint format v3 is rebuilt from stored rows when an older
  checkpoint is opened. Original stored set members are unchanged.
- Existing Boolean `Query` conditions still intersect and discard scores.
  `Retriever`, `set_similarity`, and `SearchRequest` are separate scored APIs.
  Public Kit and remote SQL Boolean surfaces reject ranked ANN, Sparse, and
  MinHash predicates; callers use the scored APIs instead.
- Exact ANN reranking is public through `POST /kit/ann_rerank`,
  `MongrelClient::kit_ann_rerank`, `ann_search_exact(...)` SQL,
  `Table.annRerank(...)` in NAPI, and `mongreldb_table_ann_rerank` in C.
- AI result cardinalities are bounded. Oversized limits, retriever lists,
  Sparse terms, MinHash members, projections, and unsafe weights return typed
  errors.
- Secured Boolean and scored SQL rank only authorized candidates. Scored table
  functions execute live instead of freezing rows during logical planning.
- Kit AI endpoints now use one JSON error envelope. The RLS candidate cache is
  byte-bounded and keyed by table-local data generation.
- Scored Kit and SQL execution now share cooperative deadlines, cancellation,
  actual-work budgets, fused-union limits, bounded blocking execution, and
  concurrency controls.
- Scored reads pin immutable table generations, so same-table readers no longer
  serialize and writers are blocked only while a changed generation is cloned.
- Native Kit reads and scored search use short-lived, HMAC-SHA-256 cursor v2
  tokens bound to the table, schema/data generation, principal, security
  version, canonical request, and first-page query time. Generation changes
  return `CURSOR_STALE`; expiry returns `CURSOR_EXPIRED`. Process-local cursor
  keys make restart and cross-instance replay fail closed.
- Hybrid search can apply an optional exact-vector post-fusion reranker while
  preserving component, fused, exact, and final scores.
- Search continuation preserves global `final_rank`. ANN adaptive over-fetch
  obeys the raw-candidate and request-specific caps; admin explain output
  reports `ann_candidate_cap_hit` when selectivity exhausts the cap.
- NAPI `aggregateExact` is the explicit secured exact aggregate. Historical
  `approxAggregate` and `incrementalAggregate` report
  `mode: "exact_fallback"`; they do not claim sampling or incremental work.
- Credentialed NAPI and C typed reads now use database-level RLS, mask, and
  live-principal enforcement. The C ABI validates integer discriminants before
  converting them to Rust types.
- Blocking and async Rust HTTP clients support centralized Bearer and Basic
  authentication for all Kit AI routes.
- Index options are backward compatible when absent. Existing defaults remain
  ANN 16/64/64 with binary-sign quantization, MinHash 128/32, and learned-range
  epsilon 16.
- Clean release qualification logs, strict JSON validation, bounded-window,
  mixed-state, RLS, encrypted, real-documentation relevance, concurrency, and
  100k AI reports are uploaded by the
  [CI qualification job](https://github.com/visorcraft/MongrelDB/actions/workflows/ci.yml).
- Tagged releases attach direct, exact-SHA evidence downloads:
  [100k clean qualification](https://github.com/visorcraft/MongrelDB/releases/latest/download/mongreldb-clean-qualification.tar.gz)
  and [1M characterization](https://github.com/visorcraft/MongrelDB/releases/latest/download/mongreldb-ai-1m-characterization.tar.gz).
- The `v0.53.3` assets and exact commit are recorded in the
  [0.53.3 qualification evidence](release-qualification-v0.53.3.md).
