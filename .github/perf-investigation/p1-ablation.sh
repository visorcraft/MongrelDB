#!/usr/bin/env bash
set -euo pipefail

: "${HEAD_SHA:?}"
: "${RUST_OLD:?}"
out=/tmp/p1-ablation
mkdir -p "$out"
exec > >(tee "$out/driver.log") 2>&1

make_variant() {
  local name=$1
  local mode=$2
  local dir="/tmp/p1-$name"
  git worktree add --detach "$dir" "$HEAD_SHA"
  python3 - "$dir" "$mode" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
modes = set(sys.argv[2].split('+'))
path = root / 'crates/mongreldb-core/src/engine.rs'
text = path.read_text()
if 'no_notnull' in modes:
    needle = '''    fn validate_columns_not_null(
        &self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
    ) -> Result<()> {
'''
    if needle not in text:
        raise SystemExit('validate_columns_not_null signature missing')
    text = text.replace(needle, needle + '        return Ok(());\n', 1)
if 'no_pk' in modes:
    needle = '''    fn bulk_pk_winner_indices(
        &self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
    ) -> Option<Vec<usize>> {
'''
    if needle not in text:
        raise SystemExit('bulk_pk_winner_indices signature missing')
    text = text.replace(needle, needle + '        return None;\n', 1)
path.write_text(text)
PY
}

make_variant normal none
make_variant no-notnull no_notnull
make_variant no-pk no_pk
make_variant no-validation no_notnull+no_pk

run_one() {
  local name=$1
  local dir="/tmp/p1-$name"
  local target="/tmp/target-p1-ablate-$name"
  (
    cd "$dir"
    CARGO_TARGET_DIR="$target" cargo "+$RUST_OLD" bench -p mongreldb-core \
      --bench scale -- 'scale/(bulk_load|bulk_load_columns|bulk_load_fast)/1000000' \
      --noplot --sample-size 10 --measurement-time 4 --warm-up-time 1
  ) 2>&1 | tee "$out/$name.log"
  while IFS= read -r estimate; do
    bench=$(python3 - "$estimate" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1]); parts = path.parts; index = parts.index('criterion')
print('/'.join(parts[index + 1:-2]))
PY
    )
    jq -c --arg variant "$name" --arg bench "$bench" \
      '{variant:$variant,bench:$bench,mean_ns:.mean.point_estimate,median_ns:.median.point_estimate}' \
      "$estimate" >> "$out/summary.jsonl"
  done < <(find "$target/criterion/scale" -path '*/1000000/new/estimates.json' | sort)
}

run_one normal
run_one no-notnull
run_one no-pk
run_one no-validation
cat "$out/summary.jsonl"
