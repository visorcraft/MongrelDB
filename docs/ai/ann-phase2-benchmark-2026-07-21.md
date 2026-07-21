# ANN Phase 2 benchmark, 2026-07-21

This reproducible smoke benchmark records build latency, peak process RSS,
serialized checkpoint size, and warm query latency for every supported ANN
algorithm and quantization combination. It is qualification evidence, not a
capacity-planning result.

Environment: Linux 7.2.0-rc3, Intel Core Ultra 9 386H, 16 logical CPUs,
rustc 1.97.1. Each backend ran in a fresh release-mode process with 512
deterministic 64-dimensional vectors, 100 queries, and `k = 10`.

| Backend | Build (µs) | Peak RSS (bytes) | Checkpoint (bytes) | Query p50 (µs) | Query p95 (µs) |
|---|---:|---:|---:|---:|---:|
| HNSW + BinarySign | 15,026 | 4,075,520 | 156,224 | 11 | 11 |
| HNSW + Dense | 85,196 | 4,526,080 | 283,192 | 48 | 51 |
| HNSW + Product | 14,285 | 3,768,320 | 82,091 | 27 | 28 |
| DiskANN + Dense | 225,933 | 5,132,288 | 345,200 | 65 | 68 |
| IVF + Dense | 2,854 | 3,821,568 | 143,883 | 7 | 8 |

`Peak RSS` is Linux `VmHWM` for the isolated process. `Checkpoint` is the exact
byte length returned by `AnnIndex::freeze`, representing persisted disk
footprint before the outer encrypted global-index envelope.

Reproduce one row with:

```bash
cargo run --release -p mongreldb-core --example ann_phase2_benchmark -- hnsw-binary
```

Valid backend arguments are `hnsw-binary`, `hnsw-dense`, `hnsw-product`,
`diskann-dense`, and `ivf-dense`.
