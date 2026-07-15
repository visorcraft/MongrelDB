#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT=${1:-"$ROOT/target/ai-generation-matrix"}
mkdir -p "$OUT"

for rows in 100000 1000000; do
  for dimension in 128 768 1536; do
    for read_kind in short long; do
      report="$OUT/concurrency-${rows}-${dimension}-${read_kind}.json"
      MONGRELDB_AI_CONCURRENCY_ROWS=$rows \
      MONGRELDB_AI_CONCURRENCY_DIM=$dimension \
      MONGRELDB_AI_CONCURRENCY_READ_KIND=$read_kind \
        cargo run -p mongreldb-core --release --all-features \
          --example ai_concurrency_bench >"$report"
      python3 "$ROOT/scripts/validate-ai-concurrency.py" "$report"
    done
  done
done

for lifetime in 0 5 30 60; do
  report="$OUT/read-generation-1m-${lifetime}s.json"
  MONGRELDB_READ_GENERATION_CURSOR_LIFETIME_SECONDS=$lifetime \
    cargo run -p mongreldb-core --release --all-features \
      --example read_generation_characterization >"$report"
  python3 "$ROOT/scripts/validate-read-generation-characterization.py" \
    "$report" "$ROOT/docs/read-generation-thresholds.json"
done

report="$OUT/ann-candidate-cap.json"
cargo run -p mongreldb-core --release --all-features \
  --example ann_candidate_cap_characterization >"$report"
python3 "$ROOT/scripts/validate-ann-candidate-cap.py" \
  "$report" "$ROOT/docs/ann-candidate-cap-thresholds.json"
