#!/usr/bin/env bash
# REM-J exact-SHA P0/P2 evidence harvester.
#
# Runs the split benchmarks in release mode for the default-standalone and
# full-feature gates and emits three JSONL files in the output directory:
#
#   p0-results.jsonl              write-path components (p0_p2_bench)
#   p2-standalone-results.jsonl   embedded + loopback point query, default build
#   p2-full-feature-results.jsonl loopback point query, cluster,oidc,vault-kms
#
# Every record carries the environment envelope required by spec §14.2:
# sha, tree state, toolchain, runner, CPU, filesystem, features. The raw
# cargo output for each run is kept beside the JSONL as <name>.log.
#
# Usage: scripts/run-p0-p2-measurements.sh OUT_DIR
set -euo pipefail

OUT_DIR="${1:?usage: scripts/run-p0-p2-measurements.sh OUT_DIR}"
mkdir -p "$OUT_DIR"
cd "$(dirname "$0")/.."

TOOLCHAIN="1.97.1"
SHA="$(git rev-parse HEAD)"
if [ -n "$(git status --porcelain)" ]; then
    TREE="dirty"
else
    TREE="clean"
fi

ENV_JSON="$(jq -n \
    --arg sha "$SHA" \
    --arg tree "$TREE" \
    --arg rustc "$(rustc "+$TOOLCHAIN" --version)" \
    --arg cargo "$(cargo "+$TOOLCHAIN" --version)" \
    --arg runner "$(hostname)" \
    --arg kernel "$(uname -sr)" \
    --arg cpu "$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1)" \
    --arg filesystem "$(df -T . | awk 'NR==2 {print $2 " on " $1}')" \
    '{sha: $sha, tree: $tree, rustc: $rustc, cargo: $cargo,
      runner: $runner, kernel: $kernel, cpu: $cpu, filesystem: $filesystem}')"

# run_bench OUT_FILE FEATURES CARGO_ARGS... — runs one release benchmark,
# harvests the JSON lines its tests print, wraps each in the env envelope,
# and appends one record per line to OUT_DIR/OUT_FILE.
run_bench() {
    local out_file="$1" features="$2" log_name="$3"
    shift 3
    local log="$OUT_DIR/$log_name.log"
    echo "== $log_name (features: ${features:-default})"
    CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-14}" \
        cargo "+$TOOLCHAIN" test --release "$@" -- --nocapture 2>&1 | tee "$log" \
        | grep -E '^\{' \
        | jq -c --argjson env "$ENV_JSON" --arg features "${features:-default}" \
            '. + {environment: ($env + {features: $features})}' \
        >> "$OUT_DIR/$out_file"
    # The benchmark run itself must have passed; grep the cargo verdict.
    grep -q "test result: ok" "$log"
}

: > "$OUT_DIR/p0-results.jsonl"
: > "$OUT_DIR/p2-standalone-results.jsonl"
: > "$OUT_DIR/p2-full-feature-results.jsonl"

# P0: write-path components, separated (table creation / first put /
# steady-state put / batch / durable commit). mongreldb-core has no
# cluster/oidc/vault-kms features; the full-feature write-path gate is
# exercised through the server build below.
run_bench p0-results.jsonl "" p0_p2_bench \
    -p mongreldb-core --test p0_p2_bench

# P2 standalone: embedded warm point query + default standalone server
# loopback point query.
run_bench p2-standalone-results.jsonl "" warm_point_query \
    -p mongreldb-core --test qualification warm_point_query_p95_baseline
run_bench p2-standalone-results.jsonl "" loopback_point_query_standalone \
    -p mongreldb-server --test scale_test loopback_point_query_p95_baseline

# P2 full-feature: same loopback benchmark against a server built with the
# cluster, oidc, and vault-kms feature set.
run_bench p2-full-feature-results.jsonl "cluster,oidc,vault-kms" loopback_point_query_full_feature \
    -p mongreldb-server --test scale_test --features cluster,oidc,vault-kms \
    loopback_point_query_p95_baseline

echo "== wrote $OUT_DIR/{p0-results,p2-standalone-results,p2-full-feature-results}.jsonl"
