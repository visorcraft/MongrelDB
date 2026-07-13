# AI retrieval qualification, 100k rows, 2026-07-12

Working-tree qualification run. Base Git SHA: `cbb0003186cd5b9b6ca59d1ef4f43605b049a820`; the audited fixes were uncommitted. Synthetic deterministic corpus. Five warmups and 50 measured queries. Linux x86-64, rustc 1.96.1, release/default.

```bash
MONGRELDB_AI_BENCH_ROWS=100000 MONGRELDB_AI_BENCH_QUERIES=50 cargo run -p mongreldb-core --release --example ai_retrieval_bench
```

```json
{"ann":{"cosine_recall_at_10":0.21999999999999997,"hamming_recall_at_10":0.7639999999999999,"p50_us":327,"p95_us":372},"base_table_bytes":23723633,"base_table_bytes_per_row":237.23633,"build_ms":15695,"checkpoint_bytes":90651205,"features":"default","git_sha":"cbb0003186cd5b9b6ca59d1ef4f43605b049a820","hardware":{"arch":"x86_64","label":"","os":"linux"},"hybrid":{"average_union_size":19.18,"p50_us":1335,"p95_us":1772},"index_bytes_per_row":906.51205,"index_payloads":[{"column_id":0,"kind":"hot_primary","payload_bytes":2400008,"payload_bytes_per_row":24.00008},{"column_id":2,"kind":"bitmap","payload_bytes":32863,"payload_bytes_per_row":0.32863},{"column_id":3,"kind":"ann","payload_bytes":30519869,"payload_bytes_per_row":305.19869},{"column_id":4,"kind":"sparse","payload_bytes":4898312,"payload_bytes_per_row":48.98312},{"column_id":5,"kind":"minhash","payload_bytes":52800024,"payload_bytes_per_row":528.00024}],"minhash":{"average_candidates":10.0,"candidate_recall_at_10":0.8139999999999997,"estimated_exact_mean_absolute_error":0.011951385498046875,"p50_us":39,"p95_us":78,"verification_p50_us":54410,"verification_p95_us":76562},"profile":"release","queries":50,"rows":100000,"rustc":"rustc 1.96.1 (31fca3adb 2026-06-26)","sparse":{"average_postings_visited":195.42,"p50_us":14,"p95_us":22},"warmup_queries":5}
```

All repository 100k latency and recall thresholds passed.

Follow-up instrumentation run, including exact-verification breakdown and dirty-tree marker:

```json
{"ann":{"cosine_recall_at_10":0.21999999999999997,"hamming_recall_at_10":0.7639999999999999,"p50_us":332,"p95_us":367},"base_table_bytes":23723633,"base_table_bytes_per_row":237.23633,"build_ms":15232,"checkpoint_bytes":90651205,"features":"default","git_dirty":true,"git_sha":"cbb0003186cd5b9b6ca59d1ef4f43605b049a820","hardware":{"arch":"x86_64","label":"","os":"linux"},"hybrid":{"average_union_size":19.18,"p50_us":1359,"p95_us":1586},"index_bytes_per_row":906.51205,"index_payloads":[{"column_id":0,"kind":"hot_primary","payload_bytes":2400008,"payload_bytes_per_row":24.00008},{"column_id":2,"kind":"bitmap","payload_bytes":32863,"payload_bytes_per_row":0.32863},{"column_id":3,"kind":"ann","payload_bytes":30519869,"payload_bytes_per_row":305.19869},{"column_id":4,"kind":"sparse","payload_bytes":4898312,"payload_bytes_per_row":48.98312},{"column_id":5,"kind":"minhash","payload_bytes":52800024,"payload_bytes_per_row":528.00024}],"minhash":{"average_candidates":10.0,"candidate_recall_at_10":0.8139999999999997,"estimated_exact_mean_absolute_error":0.011951385498046875,"p50_us":39,"p95_us":75,"verification_gather_p50_us":54955,"verification_gather_p95_us":77309,"verification_p50_us":55090,"verification_p95_us":77494,"verification_parse_p50_us":93,"verification_parse_p95_us":142,"verification_score_p50_us":23,"verification_score_p95_us":30},"profile":"release","queries":50,"rows":100000,"rustc":"rustc 1.96.1 (31fca3adb 2026-06-26)","sparse":{"average_postings_visited":195.42,"p50_us":14,"p95_us":22},"warmup_queries":5}
```
