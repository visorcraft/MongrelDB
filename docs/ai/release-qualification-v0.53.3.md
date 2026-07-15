# MongrelDB 0.53.3 exact-SHA qualification

Verified 2026-07-14 from the downloadable `v0.53.3` release assets.

- Tag commit: `f5bc902a1ffbdcc4473862b8d8e0cc3418651b99`
- Report `git_sha`: `f5bc902a1ffbdcc4473862b8d8e0cc3418651b99`
- `git_dirty`: `false`
- `qualification_mode`: `true`
- Profile: `release`
- Checkpoint inspection: `status: "ok"`, `error: null`

## Evidence

- [100k clean qualification](https://github.com/visorcraft/MongrelDB/releases/download/v0.53.3/mongreldb-clean-qualification.tar.gz), SHA-256 `f2883624a179cb6032be5bf70314a985fdb90f53f481b3d27c41608bc0eb7261`
- [1M characterization](https://github.com/visorcraft/MongrelDB/releases/download/v0.53.3/mongreldb-ai-1m-characterization.tar.gz), SHA-256 `4ddfb10afb513f89ef96bb2ce88b77f121c7a011b851de15827ffcb9055b5aba`

The clean qualification contains passing root, server, client, Node, C FFI,
sanitizer, Kit FFI, JNI, MinHash HTTP, 100k benchmark, and concurrency logs.
`scripts/validate-ai-benchmark.py` and `scripts/validate-ai-concurrency.py` both
passed. The 1M asset contains a passing structural benchmark validator for
1,000,000 rows and a passing concurrency validator.

The asset digests above match GitHub's release-asset digests. Both JSON reports
identify the exact tag commit and a clean tree.
