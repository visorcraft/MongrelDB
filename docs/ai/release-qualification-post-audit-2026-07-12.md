# AI post-audit release qualification, 2026-07-12

Qualified working tree based on `cbb0003186cd5b9b6ca59d1ef4f43605b049a820`. The tree was dirty with the audited fixes, so this is not a clean release-candidate SHA claim.

## Gates

- `cargo fmt --check`: passed
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed
- `cargo test --workspace --all-features`: 975 passed, 1 ignored, 103 suites
- server clippy: passed
- server tests: 75 passed, 10 suites
- client clippy: passed
- client tests: 5 passed, 2 suites
- Node `npm ci`: passed, 0 vulnerabilities
- Node release build: passed
- Node tests: 1 passed
- C FFI release build: passed with 18 pre-existing style/interface warnings
- C FFI release tests: 13 passed, 4 suites
- Kit FFI release build: passed
- Kit FFI release tests: 7 passed, 4 suites
- JNI release build: passed

## Benchmark qualification

- [100k report](benchmark-100k-post-audit-2026-07-12.md): 50 measured queries; repository thresholds passed.
- [1M report](benchmark-1m-post-audit-2026-07-12.md): 50 measured queries; ANN Hamming recall@10 was 0.552 and cosine recall@10 was 0.162.
- Exact MinHash verification breakdown shows gather/I/O dominates: 100k gather p95 77,309 µs versus parse 142 µs and score 30 µs; 1M gather p95 176,450 µs versus parse 236 µs and score 60 µs.

Do not claim high-recall ANN at 1M. Do not claim exact-set verification is low latency until candidate value gathering is optimized.
