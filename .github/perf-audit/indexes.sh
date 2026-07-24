#!/usr/bin/env bash
set -Eeuo pipefail

: "${TARGET_SHA:?}"
: "${RUST_VERSION:?}"

OUT=/tmp/e775-index-audit
WORK=/tmp/e775-index-worktree
mkdir -p "$OUT"
exec > >(tee -a "$OUT/driver.log") 2>&1
trap 'status=$?; echo "Index harness failed at line $LINENO: $BASH_COMMAND (status $status)"; exit $status' ERR
set -x

git worktree add --detach "$WORK" "$TARGET_SHA"
cp .github/perf-audit/index_model_audit.rs \
  "$WORK/crates/mongreldb-core/tests/index_model_audit.rs"

git archive --format=tar.gz --output="$OUT/mongreldb-e775-source.tar.gz" "$TARGET_SHA"

(
  cd "$WORK"
  set +e
  cargo "+$RUST_VERSION" fmt --check > "$OUT/fmt-check.log" 2>&1
  fmt_status=$?
  set -e
  printf '%s\n' "$fmt_status" > "$OUT/fmt-check-status.txt"
  cargo "+$RUST_VERSION" fmt --all
  git diff --stat > "$OUT/rustfmt-diff-stat.txt"
  git diff > "$OUT/rustfmt.diff"

  cargo "+$RUST_VERSION" clippy -p mongreldb-core \
    --all-targets --all-features -- -D warnings
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test index_model_audit -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test index_after_update -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features \
    --test overlay_aware_query -- --nocapture
  cargo "+$RUST_VERSION" test -p mongreldb-core --all-features
) 2>&1 | tee "$OUT/index-audit.log"
