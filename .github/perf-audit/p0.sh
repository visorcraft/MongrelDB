#!/usr/bin/env bash
set -Eeuo pipefail

: "${TARGET_SHA:?}"
: "${RUST_VERSION:?}"

BASE_SHA=eecb39fcbe9d8da7c0921db871f032a3e59e0762
PRE_SHA=bb59eb8d7071a2c08e2d0084b11c1c82cb12a521
FIX_SHA=295567a4b87956e7e990b412d65bb6ee5cb253c9
OUT=/tmp/e775-p0
mkdir -p "$OUT"
exec > >(tee -a "$OUT/driver.log") 2>&1
trap 'status=$?; echo "P0 harness failed at line $LINENO: $BASH_COMMAND (status $status)"; exit $status' ERR
set -x

prepare_variant() {
  local label=$1
  local sha=$2
  local dir="/tmp/e775-p0-$label"
  git worktree add --detach "$dir" "$sha"
  python3 - "$dir/crates/mongreldb-core/benches/write_path.rs" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
text = path.read_text()
needle = "    g.finish();\n"
if needle not in text:
    raise SystemExit(f"missing g.finish in {path}")
extra = r'''
    // Audit-only probe: measure Table::create itself, excluding tempdir creation.
    g.bench_function("table_create_only", |b| {
        b.iter_batched(
            || tempdir().unwrap(),
            |dir| black_box(Table::create(dir.path(), schema(), 1).unwrap()),
            BatchSize::SmallInput,
        );
    });

    // Audit-only probe: reuse one table so Criterion setup cannot dominate the
    // reported put cost. The growing memtable is intentional and identical
    // across revisions.
    g.bench_function("put_steady_state", |b| {
        let (_dir, mut db) = fresh_db();
        let mut i = 1_000_000_i64;
        b.iter(|| {
            black_box(
                db.put(vec![
                    (1, Value::Int64(i)),
                    (2, Value::Bytes(PAYLOAD.to_vec())),
                ])
                .unwrap(),
            );
            i += 1;
        });
    });

'''
path.write_text(text.replace(needle, extra + needle, 1))
PY
}

prepare_variant base "$BASE_SHA"
prepare_variant pre "$PRE_SHA"
prepare_variant fix "$FIX_SHA"
prepare_variant final "$TARGET_SHA"

run_variant() {
  local label=$1
  local dir="/tmp/e775-p0-$label"
  local target="/tmp/target-e775-p0-$label"
  local log="$OUT/$label.log"
  : > "$log"
  for bench in put_no_fsync table_create_only put_steady_state; do
    echo "=== $label $bench ===" | tee -a "$log"
    (
      cd "$dir"
      CARGO_TARGET_DIR="$target" cargo "+$RUST_VERSION" bench -p mongreldb-core \
        --bench write_path -- "write_path/$bench" \
        --noplot --sample-size 50 --measurement-time 3 --warm-up-time 1
    ) 2>&1 | tee -a "$log"
  done

  while IFS= read -r estimate; do
    python3 - "$label" "$estimate" <<'PY' >> "$OUT/summary.jsonl"
import json, pathlib, sys
label, filename = sys.argv[1:]
p = pathlib.Path(filename)
parts = p.parts
idx = parts.index("criterion")
bench = "/".join(parts[idx + 1:-2])
data = json.loads(p.read_text())
print(json.dumps({
    "variant": label,
    "bench": bench,
    "mean_ns": data["mean"]["point_estimate"],
    "median_ns": data["median"]["point_estimate"],
    "slope_ns": data.get("slope", {}).get("point_estimate"),
}, sort_keys=True))
PY
  done < <(find "$target/criterion/write_path" -path '*/new/estimates.json' | sort)
}

export OUT
: > "$OUT/summary.jsonl"
run_variant base
run_variant pre
run_variant fix
run_variant final
python3 - "$OUT/summary.jsonl" <<'PY' | tee "$OUT/summary.md"
import json, sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.strip()]
print("| revision | benchmark | slope/mean ns | median ns |")
print("|---|---|---:|---:|")
for row in rows:
    central=row["slope_ns"] if row["slope_ns"] is not None else row["mean_ns"]
    print(f'| {row["variant"]} | {row["bench"]} | {central:.1f} | {row["median_ns"]:.1f} |')
PY
