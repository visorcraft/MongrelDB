# AI retrieval benchmark methodology

Run the deterministic manual/nightly harness in release mode:

```bash
MONGRELDB_AI_BENCH_ROWS=100000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --all-features --example ai_retrieval_bench

MONGRELDB_AI_BENCH_ROWS=1000000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --all-features --example ai_retrieval_bench
```

Release qualification is strict and validates the emitted JSON:

```bash
MONGRELDB_AI_QUALIFICATION=1 \
MONGRELDB_AI_BENCH_ROWS=100000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --all-features --example ai_retrieval_bench \
  > ai-benchmark-100k.json
python3 scripts/validate-ai-benchmark.py \
  ai-benchmark-100k.json docs/ai/benchmark-thresholds.json \
  --expected-sha "$(git rev-parse HEAD)"
```

Strict mode exits nonzero when checkpoint metadata or payload inspection fails.
The validator checks SHA, clean tree, release profile, corpus size, required
HOT/Bitmap/ANN/Sparse/MinHash payloads, finite values, the 10/50/100/200 exact-rerank
matrix, and every threshold in `benchmark-thresholds.json`.

The scheduled `AI 1M qualification` workflow runs the same strict checkpoint
and report validation at one million rows. It skips 100k-specific latency
thresholds and uploads the clean report as a final-SHA artifact.

The JSON report records git SHA, OS/architecture, corpus size, build time,
base sorted-run bytes, per-index checkpoint payload bytes, p50/p95 latency, ANN graph recall against
exhaustive Hamming, end-to-end ANN recall against exhaustive cosine, and
filtered hybrid latency, exact ANN rerank latency/recall, and explicit
checkpoint inspection status. Hybrid output includes candidate, hard-filter,
fusion, and projection p95 stage timings. Sparse and MinHash use deterministic
corpus/query shapes. Qualification also proves a 100,000-candidate query only
projects its 20-row ranked window, then measures clean and operational layouts,
5% updates, 1% deletes, TTL, nine immutable runs, hot and mutable tiers,
candidate-aware RLS at 1%, 10%, and 50% selectivity, column masks, encrypted AI
columns, and a labeled support-retrieval corpus. No pull-request test asserts
wall-clock thresholds.

Publish reports with named CPU, memory, compiler, build features, warmup policy,
and the unmodified JSON output. Do not compare index bytes with base-table
columnar bytes as if they measured the same object. Establish a baseline before
adding regression thresholds. A 10M-row run is optional release qualification.
