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
- Hybrid search can apply an optional exact-vector post-fusion reranker while
  preserving component, fused, exact, and final scores.
- Credentialed NAPI and C typed reads now use database-level RLS, mask, and
  live-principal enforcement. The C ABI validates integer discriminants before
  converting them to Rust types.
- Blocking and async Rust HTTP clients support centralized Bearer and Basic
  authentication for all Kit AI routes.
- Index options are backward compatible when absent. Existing defaults remain
  ANN 16/64/64 with binary-sign quantization, MinHash 128/32, and learned-range
  epsilon 16.
- Clean release qualification logs, strict JSON validation, bounded-window,
  mixed-state, RLS, encrypted, realistic-corpus, and 100k AI reports are uploaded by
  the [CI qualification job](https://github.com/visorcraft/MongrelDB/actions/workflows/ci.yml).
