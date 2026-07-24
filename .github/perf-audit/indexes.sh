#!/usr/bin/env bash
set -euo pipefail

: "${TARGET_SHA:?}"
: "${RUST_VERSION:?}"

OUT=/tmp/e775-index-audit
WORK=/tmp/e775-index-worktree
mkdir -p "$OUT"
git worktree add --detach "$WORK" "$TARGET_SHA"
cp .github/perf-audit/index_model_audit.rs \
  "$WORK/crates/mongreldb-core/tests/index_model_audit.rs"

git archive --format=tar.gz --output="$OUT/mongreldb-e775-source.tar.gz" "$TARGET_SHA"

(
  cd "$WORK"
  cargo "+$RUST_VERSION" fmt --check
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
