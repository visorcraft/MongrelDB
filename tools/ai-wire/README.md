# AI wire smoke tools

Start a release server against a fresh database, then run one script:

```bash
cargo run --manifest-path crates/mongreldb-server/Cargo.toml --release -- --db /tmp/mongreldb-ai-wire
python3 tools/ai-wire/demo_ann.py
python3 tools/ai-wire/demo_sparse.py
python3 tools/ai-wire/demo_minhash.py
```

Override `MONGRELDB_URL`, `MONGRELDB_DB_PATH`, `MONGRELDB_PROFILE`, and
`MONGRELDB_FEATURES` when needed. Fixtures are deterministic. Each script uses
a separate table and prints environment metadata plus the exact request and
response on failure. Use a fresh database for repeated runs.

| Capability | Rust core | SQL declaration | Kit declaration | Kit insert | Kit query | Reopen |
|---|---:|---:|---:|---:|---:|---:|
| ANN | yes | yes | yes | yes | yes | yes |
| Sparse | yes | yes | yes | yes | yes | yes |
| MinHash | yes | yes | yes | yes | yes | yes |

Failures should be classified as unsupported declaration, invalid wire value,
index not populated, query not parsed, wrong JSON variant, empty candidate set,
candidate discarded by intersection, or reopen/checkpoint failure.
