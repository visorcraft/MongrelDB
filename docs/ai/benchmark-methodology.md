# AI retrieval benchmark methodology

Run the deterministic manual/nightly harness in release mode:

```bash
MONGRELDB_AI_BENCH_ROWS=100000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --example ai_retrieval_bench

MONGRELDB_AI_BENCH_ROWS=1000000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --example ai_retrieval_bench
```

The JSON report records git SHA, OS/architecture, corpus size, build time,
base sorted-run bytes, per-index checkpoint payload bytes, p50/p95 latency, ANN graph recall against
exhaustive Hamming, end-to-end ANN recall against exhaustive cosine, and
filtered hybrid latency. Sparse and MinHash use deterministic corpus/query
shapes. No pull-request test asserts wall-clock thresholds.

Publish reports with named CPU, memory, compiler, build features, warmup policy,
and the unmodified JSON output. Do not compare index bytes with base-table
columnar bytes as if they measured the same object. Establish a baseline before
adding regression thresholds. A 10M-row run is optional release qualification.
