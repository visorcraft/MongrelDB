#!/usr/bin/env bash
set -euo pipefail

: "${HEAD_SHA:?}"
: "${QUAL_BASE:?}"
: "${RUST_OLD:?}"
: "${RUST_NEW:?}"
out=/tmp/p2-exact
mkdir -p "$out"
git worktree add --detach /tmp/p2-base "$QUAL_BASE"
git worktree add --detach /tmp/p2-head "$HEAD_SHA"

run_one() {
  local dir=$1 label=$2 toolchain=$3 target="/tmp/target-p2-${label}-${toolchain//./_}"
  local log="$out/${label}-${toolchain}.log"
  for repetition in 1 2 3; do
    echo "=== repetition $repetition ===" | tee -a "$log"
    (
      cd "$dir/crates/mongreldb-server"
      CARGO_TARGET_DIR="$target" cargo "+$toolchain" test --release \
        --test scale_test loopback_point_query_p95_baseline -- --nocapture
    ) 2>&1 | tee -a "$log"
  done
  grep '"test":"loopback_point_query"' "$log" > "$out/${label}-${toolchain}.jsonl" || true
}

run_one /tmp/p2-base base "$RUST_OLD"
run_one /tmp/p2-head head "$RUST_OLD"
run_one /tmp/p2-head head "$RUST_NEW"
cat "$out"/*.jsonl
