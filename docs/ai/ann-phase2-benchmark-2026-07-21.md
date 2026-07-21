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
| HNSW + BinarySign | 40,016 | 4,136,960 | 156,224 | 44 | 49 |
| HNSW + Dense | 72,122 | 4,534,272 | 283,192 | 55 | 58 |
| Flat Product | 13,305 | 3,772,416 | 82,091 | 26 | 27 |
| DiskANN + Dense | 201,607 | 5,378,048 | 385,392 | 68 | 70 |
| IVF + Dense | 2,781 | 3,870,720 | 143,883 | 6 | 7 |

`Peak RSS` is Linux `VmHWM` for the isolated process. `Checkpoint` is the exact
byte length returned by `AnnIndex::freeze`, representing persisted disk
footprint before the outer encrypted global-index envelope.

Reproduce one row with:

```bash
cargo run --release -p mongreldb-core --example ann_phase2_benchmark -- hnsw-binary
```

Valid backend arguments are `hnsw-binary`, `hnsw-dense`, `flat-product`,
`diskann-dense`, and `ivf-dense`.

These rows replace the superseded positive-only generator run and identify the
Product backend as flat PQ rather than HNSW.
