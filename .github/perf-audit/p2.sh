#!/usr/bin/env bash
set -euo pipefail

: "${TARGET_SHA:?}"
: "${RUST_VERSION:?}"

BASE_SHA=edb690b115846681f5149b2bd56fc92b05ff6166
PRE_SHA=bb59eb8d7071a2c08e2d0084b11c1c82cb12a521
PARSE_FIX_SHA=fabcb3fa2f39bcbd2c7f06985529482ba095ddce
GATE_FIX_SHA=069607eed236ad50cf6e093f76c40b3be2906660
OUT=/tmp/e775-p2
TARGET=/tmp/target-e775-p2-shared
mkdir -p "$OUT"

add_worktree() {
  local label=$1 sha=$2
  git worktree add --detach "/tmp/e775-p2-$label" "$sha"
}

add_worktree base "$BASE_SHA"
add_worktree pre "$PRE_SHA"
add_worktree parse-fix "$PARSE_FIX_SHA"
add_worktree gate-fix "$GATE_FIX_SHA"
add_worktree final "$TARGET_SHA"

run_variant() {
  local label=$1 features=${2:-}
  local dir="/tmp/e775-p2-${label%%-*}"
  # Labels with a suffix (final-full) still run from the final worktree.
  if [[ "$label" == final-full ]]; then
    dir=/tmp/e775-p2-final
  fi
  local log="$OUT/$label.log"
  : > "$log"
  for repetition in 1 2 3; do
    echo "=== $label repetition $repetition ===" | tee -a "$log"
    (
      cd "$dir/crates/mongreldb-server"
      if [[ -n "$features" ]]; then
        CARGO_TARGET_DIR="$TARGET" cargo "+$RUST_VERSION" test --release \
          --features "$features" --test scale_test \
          loopback_point_query_p95_baseline -- --nocapture
      else
        CARGO_TARGET_DIR="$TARGET" cargo "+$RUST_VERSION" test --release \
          --test scale_test loopback_point_query_p95_baseline -- --nocapture
      fi
    ) 2>&1 | tee -a "$log"
  done
  grep '"test":"loopback_point_query"' "$log" > "$OUT/$label.jsonl"
}

# Interleave the important endpoints to reduce runner drift. Intermediate
# revisions then attribute how much came from duplicate-parse removal versus
# feature-gating cold code.
run_variant base
run_variant final
run_variant pre
run_variant gate-fix
run_variant parse-fix
run_variant final-full 'cluster,native-rpc,remote-embedding,oidc,vault-kms'

python3 - "$OUT" <<'PY' | tee "$OUT/summary.md"
import json, pathlib, statistics, sys
root=pathlib.Path(sys.argv[1])
print("| revision | p50 median us | p95 median us | p99 median us | runs |")
print("|---|---:|---:|---:|---:|")
summary=[]
for path in sorted(root.glob("*.jsonl")):
    rows=[json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    label=path.stem
    values={key:[row["point_query_latency"][key] for row in rows] for key in ("p50_us","p95_us","p99_us")}
    item={
        "variant":label,
        "runs":len(rows),
        "p50_us":statistics.median(values["p50_us"]),
        "p95_us":statistics.median(values["p95_us"]),
        "p99_us":statistics.median(values["p99_us"]),
    }
    summary.append(item)
    print(f'| {label} | {item["p50_us"]:.3f} | {item["p95_us"]:.3f} | {item["p99_us"]:.3f} | {len(rows)} |')
(root/"summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
PY
