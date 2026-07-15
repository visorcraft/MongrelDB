# AI retrieval benchmark methodology

Run the deterministic manual/nightly harness in release mode:

```bash
MONGRELDB_AI_BENCH_ROWS=100000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --all-features --example ai_retrieval_bench

MONGRELDB_AI_BENCH_ROWS=1000000 \
MONGRELDB_AI_BENCH_QUERIES=50 \
cargo run -p mongreldb-core --release --all-features --example ai_retrieval_bench

MONGRELDB_AI_CONCURRENCY_ROWS=10000 \
MONGRELDB_AI_CONCURRENCY_OPS=25 \
cargo run -p mongreldb-core --release --all-features --example ai_concurrency_bench \
  > ai-concurrency.json

scripts/run-ai-generation-matrix.sh target/ai-generation-matrix
```

The matrix runner covers 100k and 1M rows, 128/768/1536-dimensional
embeddings, short and long scored reads, 1/4/16/32 readers, 0/1/4 writers,
and retained read generations lasting 0/5/30/60 seconds. Each report includes
query and commit p50/p95/p99, throughput, peak RSS, clone bytes/write, writer
wait time, and explicit failure status. It also runs the >250,000-row ANN cap
memory qualification with sub-1% RLS and exact reranking.

Changing an embedding dimension is only allowed before stored versions exist.
A table that can issue an ANN cursor already has stored embeddings, so the DDL
is rejected without changing its generation. Add, drop, and ANN-option
replacement tests cover successful AI-index schema changes between pages.

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

The tagged GitHub Actions qualification uses `ci-benchmark-thresholds.json`.
Hosted runners have variable CPUs, so those ceilings are conservative regression
guards. `benchmark-thresholds.json` remains the stricter reference-machine gate.

The scheduled `AI 1M characterization` workflow runs strict structural and
finite-value validation at one million rows. It skips latency/recall thresholds,
so it is characterization evidence, not a performance qualification gate.

## AI work units

`max_work` is monotonic across the dominant loops. One unit represents one
posting/candidate/set-member visit, one projected cell, or 64 float, packed-byte,
signature, or encoded-byte operations. MinHash bucket candidates are also capped
at 250,000. Reducing `max_work` therefore reduces CPU and intermediate memory.

Remote Boolean ANN, Sparse, and MinHash predicates are rejected. Use scored Kit
or scored SQL functions, which apply the shared deadline, semaphore, cancellation,
work-budget, and candidate-limit contract. Native offsets above 100,000 are
rejected. Kit native reads return a snapshot-pinned `next_cursor`; use it for
large exports.

NAPI historical reads evaluate the current principal and current security
catalog against historical row values. Current RLS, column grants, and masks
therefore apply. NAPI `aggregateExact` is exact over authorized rows.
`approxAggregate` uses the bounded reservoir and reports `mode: "approximate"`
with a z-controlled confidence interval after grants, RLS, and masks.
`incrementalAggregate` reports `mode: "incremental"` and reuses the append-only
delta cache; it returns an explicit unsupported error while RLS or masks are
active. NAPI and C writes use live-principal database transactions; RLS `USING`/`WITH CHECK` and
column grants apply at commit. Typed NAPI bulk load is admin-only.

On credentialless databases, `explain=true` is available because authorization
is disabled. On credentialed databases, Admin is rechecked inside every scored
read retry. Failed authorization responses never include the trace.

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
columns, and a labeled support-retrieval smoke corpus. The real relevance suite
indexes sections from the versioned MongrelDB technical documentation and
reports Recall@10, MRR@10, NDCG@10, duplicate suppression, answer-context
coverage, latency, and index size for dense-only, sparse-only, RRF, and RRF plus
exact-vector reranking. The concurrency harness measures 1/4/16/32 readers
against 0/1/4 writers with RLS and exact reranking, including p50/p95/p99 query
and commit latency, throughput, and peak RSS. No pull-request test asserts
wall-clock thresholds.

Publish reports with named CPU, memory, compiler, build features, warmup policy,
and the unmodified JSON output. Do not compare index bytes with base-table
columnar bytes as if they measured the same object. Measure the reference
machine before adding regression thresholds. A 10M-row run is optional release
qualification.
