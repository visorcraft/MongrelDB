# Contributing to MongrelDB

Thanks for taking the time to help MongrelDB. This document describes how
to propose a change, what we expect from a pull request, and the coding
standards that apply to the codebase.

If anything here is unclear or out of date, open an issue or a PR.

## Code of conduct

Be kind, be specific, assume good faith. Disagree about the technical
details, not the person. Public reviews stay focused on the diff.

## How to propose a change

MongrelDB uses a standard **fork → branch → pull request** workflow on
GitHub.

1. **Fork** [`visorcraft/MongrelDB`](https://github.com/visorcraft/MongrelDB)
   to your GitHub account.
2. **Clone** your fork and add the upstream remote:

   ```sh
   git clone git@github.com:<you>/MongrelDB.git
   cd MongrelDB
   git remote add upstream https://github.com/visorcraft/MongrelDB.git
   ```

3. **Branch** from `master`. Pick a descriptive, kebab-case branch name:
   `fix-result-cache-stale-hit`, `feature/broadcast-join`, `docs/encryption-guide`.

   ```sh
   git fetch upstream
   git switch -c my-change upstream/master
   ```

4. **Make focused commits.** One logical change per commit. Run the
   preflight (see below) before pushing.
5. **Open a pull request** against `master` on `visorcraft/MongrelDB`.
   Fill in the PR template:
   - **What.** One paragraph summary of the change.
   - **Why.** Bug fix? New feature? Doc fix? Link the issue if one
     exists.
   - **How to test.** The exact commands a reviewer should run.
   - **Risk.** What might break? What did you not test?

## Before you push: preflight

Run the full CI preflight locally:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

All three must pass with zero warnings. If a check fails, fix the root
cause - don't `--no-verify`, don't silence clippy lints with `#[allow(...)]`
unless you justify it in the PR description.

If you touched performance-sensitive code, also run the relevant benchmarks:

```sh
cargo bench -p mongreldb-core --bench scale           # ingest + scan throughput
cargo bench -p mongreldb-core --bench filtered_query   # pushdown filter speed
cargo run -p mongreldb-perf --release --bin compare mongrel  # cross-engine matrix
```

## What we look for in a review

- The change does one thing and does it well.
- Behavior changes ship with tests. New `mongreldb-core` API: a unit
  test in `src/` or `tests/`. New SQL feature: a test in
  `crates/mongreldb-query/tests/`. New NAPI method: verify it
  compiles with `cargo check` in `crates/mongreldb-node`.
- The change keeps `mongreldb-core` as the source of truth. The query
  crate and NAPI addon are clients of core; don't re-implement storage,
  indexing, or WAL logic outside of core.
- Documentation is updated alongside the code (`docs/`, `BENCHMARKS.md`,
  `README.md`) if the change affects users.
- Commits have clear messages (see below).

## Coding standards

### Rust

- **Edition / toolchain.** Rust 2021, MSRV 1.80. Don't bump the MSRV
  casually.
- **Formatting.** `cargo fmt --all`. No custom `rustfmt.toml` - the
  default style is canonical.
- **Linting.** `cargo clippy --workspace --all-targets --all-features
  -- -D warnings` must pass with no warnings.
- **Errors.** Return the project's `Result<T>` type. Use `thiserror`
  variants (`MongrelError`, `MongrelQueryError`). No `String`-typed
  errors at API boundaries.
- **Panics.** Don't panic from library code (`mongreldb-core` /
  `mongreldb-query`). Use `Result`. `unwrap` / `expect` is acceptable
  in tests and in standalone binaries only.
- **Unsafe.** New `unsafe` blocks require a comment explaining the
  invariant they uphold. The codebase uses `unsafe` sparingly (mainly
  in `bytemuck` casts and `mmap`).
- **Synchronous I/O.** Core uses `std::fs` (synchronous). Don't
  introduce `tokio` or async into core. The query crate uses tokio
  only for DataFusion's async runtime.
- **Dependencies.** Prefer crates that already appear in `Cargo.lock`.
  New dependencies must be MIT or Apache-2.0 licensed.

### NAPI addon (mongreldb-node)

- Every blocking method has an `*Async` variant that offloads to
  `spawn_blocking`.
- Row IDs, counts, and epochs cross the FFI as `BigInt` (not JS Number).
- Single-row `put` stays synchronous and durable - never route it
  through a buffer or background thread.

### Commit messages

- Subject line: imperative mood, ≤ 72 characters, no trailing period.
  Example: `Add broadcast join via bitmap-key probe of PK HOT`.
- Body: wrap at 72 characters. Explain *why*, not *what* (the diff
  shows the what).
- Reference issues with `Fixes #123` / `Refs #123` on a final line
  when applicable.
- **Never** add AI/assistant attribution (no `Co-Authored-By`, no
  `Generated with`, no tool names).

## Issue reports

A useful bug report includes:

- MongrelDB version (from `Cargo.toml`).
- Your OS and Rust version (`rustc --version`).
- The exact code or commands that reproduce the issue.
- The expected result and the actual result.
- Any error messages or panics (include the full backtrace with
  `RUST_BACKTRACE=1`).

Feature requests are welcome. Please describe the problem you're trying
to solve before proposing the solution.

## Releases

1. Bump `workspace.package.version` in `Cargo.toml`.
2. Update `BENCHMARKS.md` if performance numbers changed.
3. `git commit`, then `git tag -a vX.Y.Z -m "vX.Y.Z"` (GPG-signed).
4. `git push origin master && git push origin vX.Y.Z`.

## Security

If you find a vulnerability, **do not** open a public GitHub issue.
Report it privately through GitHub's private vulnerability reporting -
the repository's **Security** tab → **Report a vulnerability**. The full
policy is in [`SECURITY.md`](SECURITY.md).

## Licensing

MongrelDB is dual-licensed under MIT OR Apache-2.0. By contributing,
you agree that your changes are made available under the same license.

- Do **not** paste code from other database engines unless you have
  done a license review first.
- New third-party dependencies must be MIT or Apache-2.0 licensed.

Thanks again - looking forward to your PR.
