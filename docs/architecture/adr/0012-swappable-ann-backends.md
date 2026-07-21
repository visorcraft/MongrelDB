# ADR-0012: Swappable ANN Backends

- Status: Accepted
- Date: 2026-07-21
- Spec references: Phase 2 Dense ANN and swappable algorithms program

## Context

MongrelDB shipped a single ANN implementation: HNSW over two quantizations
(BinarySign Hamming, Dense cosine). Every ANN index used the same graph
algorithm. Customers asked for:

- product quantization (PQ) to compress large embedding sets, and
- alternative graph algorithms (DiskANN, IVF) tuned for larger-than-memory or
  different recall/latency tradeoffs.

The original `AnnIndex` dispatched on a private `AnnBody` enum with one arm per
quantization. Every method (`insert`, `search`, `freeze`, `thaw`, `seal`,
`merge_deltas_into_base`) matched on it, so adding an algorithm or quantization
meant reopening the orchestrator and risked subtle behavior drift across the
six call sites.

## Decision

1. Introduce a `pub(crate) trait AnnBackend` covering the operations the
   orchestrator performs: insert, search, search_filtered, entries, freeze,
   thaw, seal/consolidate, and clone. The orchestrator (`AnnIndex`) holds an
   `active: Box<dyn AnnBackend>` mutable delta plus a
   `frozen: Vec<Arc<dyn AnnBackend>>` immutable base/delta list. Every former
   `match` arm on `AnnBody` collapses to a single trait call.

2. The algorithm (hnsw / diskann / ivf) and the quantization
   (binary_sign / dense / product) are **independent** choices on
   `AnnOptions`. The algorithm chooses how search walks the index; the
   quantization chooses how vectors are represented. `validate_options`
   rejects unsupported combinations fail-closed (a typed Schema error),
   never a silent fallback to HNSW.

3. Each concrete backend is a separate module implementing `AnnBackend`:
   - `Hnsw` / `DenseHnsw` (BinarySign Hamming, Dense cosine) — the original
     implementations, now behind the trait.
   - `PqBackend` — flat product quantization: active delta buffers Dense
     vectors, trains the codebook at freeze, emits compact codes. Search is
     bounded ADC + optional exact rerank.
   - `DiskAnnBackend` — Vamana single-layer robust-pruned graph.
   - `IvfBackend` — k-means centroids + inverted lists.

4. Determinism is mandatory. Every backend's build is reproducible for a fixed
   seed + fixed insertion order, so consolidation (rebuild a base from the union
   of frozen layers) produces a byte-identical result to a base-only build.
   This preserves the existing merge/consolidation guarantee.

5. Checkpoint versioning: `_idx/global.idx` format bumped 4 → 5. Prior-format
   files are discarded at open and rebuilt from authoritative schema + sorted
   runs (the existing `format_version` escape hatch). The checkpoint payload
   carries one variant per backend; the outer envelope carries the quantization
   tag so a mismatch fails closed. PQ codebooks and IVF centroids reuse the
   existing GCM encryption envelope — no new crypto path.

6. Memory admission (`reserve_hidden_memory`) branches on algorithm so each
   backend's cost model is accurate: HNSW layered adjacency, DiskAnn degree-R
   single-layer, IVF per-node vectors + centroids, PQ Dense-training buffer.

## Consequences

- Adding a future algorithm or quantization is a new `AnnBackend` impl plus one
  `new_backend` arm and one `validate_options` arm. The orchestrator never
  reopens.
- The orchestrator's base+delta exact-rerank guarantee (each layer contributes
  candidates, merged keeping each row's minimum distance, truncated to k) is
  preserved across all backends because it lives above the trait.
- Recall is backend-specific and verified against brute force in the
  `index::ann::matrix` test matrix; thresholds are documented per combination.
- DiskANN's "disk" in our embedded model is the in-memory graph bounded by the
  memory governor reservation; there is no separate on-disk vector file (the
  sorted-run pages already serve that role via the spill/temp-disk budget).
