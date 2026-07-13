# AI retrieval qualification, 1M rows, 2026-07-12

Working-tree qualification run. Base Git SHA: `cbb0003186cd5b9b6ca59d1ef4f43605b049a820`; the audited fixes were uncommitted. Synthetic deterministic corpus. Five warmups and 50 measured queries. Linux x86-64, rustc 1.96.1, release/default.

```bash
MONGRELDB_AI_BENCH_ROWS=1000000 MONGRELDB_AI_BENCH_QUERIES=50 cargo run -p mongreldb-core --release --example ai_retrieval_bench
```

```json
{"ann":{"cosine_recall_at_10":0.162,"hamming_recall_at_10":0.5520000000000002,"p50_us":464,"p95_us":545},"base_table_bytes":236818399,"base_table_bytes_per_row":236.818399,"build_ms":336187,"checkpoint_bytes":905380349,"features":"default","git_sha":"cbb0003186cd5b9b6ca59d1ef4f43605b049a820","hardware":{"arch":"x86_64","label":"","os":"linux"},"hybrid":{"average_union_size":19.9,"p50_us":6270,"p95_us":7763},"index_bytes_per_row":905.380349,"index_payloads":[{"column_id":0,"kind":"hot_primary","payload_bytes":24000008,"payload_bytes_per_row":24.000008},{"column_id":2,"kind":"bitmap","payload_bytes":262463,"payload_bytes_per_row":0.262463},{"column_id":3,"kind":"ann","payload_bytes":305019413,"payload_bytes_per_row":305.019413},{"column_id":4,"kind":"sparse","payload_bytes":48098312,"payload_bytes_per_row":48.098312},{"column_id":5,"kind":"minhash","payload_bytes":528000024,"payload_bytes_per_row":528.000024}],"minhash":{"average_candidates":10.0,"candidate_recall_at_10":1.0,"estimated_exact_mean_absolute_error":0.0,"p50_us":211,"p95_us":461,"verification_p50_us":168839,"verification_p95_us":172103},"profile":"release","queries":50,"rows":1000000,"rustc":"rustc 1.96.1 (31fca3adb 2026-06-26)","sparse":{"average_postings_visited":1953.16,"p50_us":69,"p95_us":75},"warmup_queries":5}
```

At 1M, Hamming recall@10 was 0.552 and cosine recall@10 was 0.162. Do not claim high-recall ANN at this scale without exact reranking or higher ANN breadth.

Follow-up instrumentation run, including exact-verification breakdown and dirty-tree marker:

```json
{"ann":{"cosine_recall_at_10":0.162,"hamming_recall_at_10":0.5520000000000002,"p50_us":478,"p95_us":591},"base_table_bytes":236818399,"base_table_bytes_per_row":236.818399,"build_ms":350074,"checkpoint_bytes":905380349,"features":"default","git_dirty":true,"git_sha":"cbb0003186cd5b9b6ca59d1ef4f43605b049a820","hardware":{"arch":"x86_64","label":"","os":"linux"},"hybrid":{"average_union_size":19.9,"p50_us":6406,"p95_us":7461},"index_bytes_per_row":905.380349,"index_payloads":[{"column_id":0,"kind":"hot_primary","payload_bytes":24000008,"payload_bytes_per_row":24.000008},{"column_id":2,"kind":"bitmap","payload_bytes":262463,"payload_bytes_per_row":0.262463},{"column_id":3,"kind":"ann","payload_bytes":305019413,"payload_bytes_per_row":305.019413},{"column_id":4,"kind":"sparse","payload_bytes":48098312,"payload_bytes_per_row":48.098312},{"column_id":5,"kind":"minhash","payload_bytes":528000024,"payload_bytes_per_row":528.000024}],"minhash":{"average_candidates":10.0,"candidate_recall_at_10":1.0,"estimated_exact_mean_absolute_error":0.0,"p50_us":236,"p95_us":456,"verification_gather_p50_us":170472,"verification_gather_p95_us":176450,"verification_p50_us":170963,"verification_p95_us":176981,"verification_parse_p50_us":210,"verification_parse_p95_us":236,"verification_score_p50_us":53,"verification_score_p95_us":60},"profile":"release","queries":50,"rows":1000000,"rustc":"rustc 1.96.1 (31fca3adb 2026-06-26)","sparse":{"average_postings_visited":1953.16,"p50_us":68,"p95_us":75},"warmup_queries":5}
```
