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
for probe in index_model_audit index_failure_probes clustered_index_probe stale_hot_probe; do
  cp ".github/perf-audit/$probe.rs" \
    "$WORK/crates/mongreldb-core/tests/$probe.rs"
done

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

  run_diagnostic() {
    local test_name=$1
    if cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
      --test "$test_name" -- --nocapture \
      > "$OUT/$test_name.log" 2>&1; then
      status=0
    else
      status=$?
    fi
    printf '%s\n' "$status" > "$OUT/$test_name-status.txt"
  }

  run_diagnostic stale_hot_probe
  run_diagnostic index_failure_probes
  run_diagnostic clustered_index_probe
  run_diagnostic index_model_audit

  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test index_after_update -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test overlay_aware_query -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features
) 2>&1 | tee "$OUT/index-audit.log"
