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
- Index options are backward compatible when absent. Existing defaults remain
  ANN 16/64/64 with binary-sign quantization, MinHash 128/32, and learned-range
  epsilon 16.
