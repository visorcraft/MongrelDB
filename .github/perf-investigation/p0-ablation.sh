#!/usr/bin/env bash
set -euo pipefail

: "${HEAD_SHA:?}"
: "${RUST_OLD:?}"
out=/tmp/p0-ablation
mkdir -p "$out"

make_variant() {
  local name=$1 mode=$2 dir="/tmp/p0-$name"
  git worktree add --detach "$dir" "$HEAD_SHA"
  python3 - "$dir" "$mode" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
modes = set(sys.argv[2].split('+'))

def replace(path, old, new):
    text = path.read_text()
    if old not in text:
        raise SystemExit(f"missing replacement in {path}: {old[:80]!r}")
    path.write_text(text.replace(old, new, 1))

if "no_typecheck" in modes:
    replace(
        root / "crates/mongreldb-core/src/schema.rs",
        '''            if !value_matches_type(value, column.ty.clone()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "column '{}' ({}) value {value:?} does not match type {:?}",
                    column.name, column.id, column.ty
                )));
            }
''',
        ''
    )

if "unchecked_rowid" in modes:
    replace(
        root / "crates/mongreldb-core/src/rowid.rs",
        '''        self.next = self
            .next
            .checked_add(1)
            .filter(|next| *next < u64::MAX)
            .ok_or_else(row_id_exhausted)?;
        Ok(RowId(id))
''',
        '''        self.next = self.next.wrapping_add(1);
        Ok(RowId(id))
'''
    )

if "no_private_flags" in modes:
    engine = root / "crates/mongreldb-core/src/engine.rs"
    replace(engine, '''                w.append_txn(txn_id, op)?;
                self.pending_private_mutations = true;
''', '''                w.append_txn(txn_id, op)?;
''')
    replace(engine, '''        if self.durable_commit_failed {
            return Err(MongrelError::Other(
                "table poisoned by post-commit failure; reopen required".into(),
            ));
        }
''', '')

if "unchecked_wal_seq" in modes:
    replace(
        root / "crates/mongreldb-core/src/wal.rs",
        '''        let next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL sequence namespace exhausted".into()))?;
''',
        '''        let next_seq = self.next_seq.wrapping_add(1);
'''
    )
PY
  cargo "+$RUST_OLD" fmt --manifest-path "$dir/Cargo.toml"
}

make_variant normal none
make_variant no-typecheck no_typecheck
make_variant unchecked-rowid unchecked_rowid
make_variant no-private-flags no_private_flags
make_variant unchecked-wal-seq unchecked_wal_seq
make_variant combined no_typecheck+unchecked_rowid+no_private_flags+unchecked_wal_seq

run_one() {
  local name=$1 dir="/tmp/p0-$name" target="/tmp/target-p0-ablate-$name"
  (
    cd "$dir"
    CARGO_TARGET_DIR="$target" cargo "+$RUST_OLD" bench -p mongreldb-core \
      --bench write_path -- write_path/put_no_fsync \
      --noplot --sample-size 80 --measurement-time 5 --warm-up-time 2
  ) 2>&1 | tee "$out/$name.log"
  estimate=$(find "$target/criterion" -path '*/write_path/put_no_fsync/new/estimates.json' -print -quit)
  jq -c --arg variant "$name" \
    '{variant:$variant,mean_ns:.mean.point_estimate,median_ns:.median.point_estimate,slope_ns:(.slope.point_estimate // null)}' \
    "$estimate" | tee -a "$out/summary.jsonl"
}

run_one normal
run_one no-typecheck
run_one unchecked-rowid
run_one no-private-flags
run_one unchecked-wal-seq
run_one combined
cat "$out/summary.jsonl"
