#!/usr/bin/env bash
set -euo pipefail

: "${TARGET_SHA:?}"
: "${RUST_VERSION:?}"

OUT=/tmp/e775-index-audit
WORK=/tmp/e775-index-worktree
mkdir -p "$OUT"
exec > >(tee -a "$OUT/driver.log") 2>&1
set -x

git worktree add --detach "$WORK" "$TARGET_SHA"
cp .github/perf-audit/index_model_audit.rs \
  "$WORK/crates/mongreldb-core/tests/index_model_audit.rs"
cp .github/perf-audit/index_failure_probes.rs \
  "$WORK/crates/mongreldb-core/tests/index_failure_probes.rs"

git archive --format=tar.gz --output="$OUT/mongreldb-e775-source.tar.gz" "$TARGET_SHA"

(
  cd "$WORK"
  if cargo "+$RUST_VERSION" fmt --check > "$OUT/fmt-check.log" 2>&1; then
    fmt_status=0
  else
    fmt_status=$?
  fi
  printf '%s\n' "$fmt_status" > "$OUT/fmt-check-status.txt"
  cargo "+$RUST_VERSION" fmt --all
  git diff --stat > "$OUT/rustfmt-diff-stat.txt"
  git diff > "$OUT/rustfmt.diff"

  cargo "+$RUST_VERSION" clippy -p mongreldb-core \
    --all-targets --all-features -- -D warnings
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test index_model_audit -- --nocapture

  # Focused probes are diagnostic: record all failures, then continue through
  # the repository's established index and complete-core suites.
  if cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test index_failure_probes -- --nocapture \
    > "$OUT/index-failure-probes.log" 2>&1; then
    probe_status=0
  else
    probe_status=$?
  fi
  printf '%s\n' "$probe_status" > "$OUT/index-failure-probes-status.txt"

  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test index_after_update -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test overlay_aware_query -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features
) 2>&1 | tee "$OUT/index-audit.log"
