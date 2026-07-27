#!/usr/bin/env bash
# REM-J / B468-10 exact-SHA P0/P2 evidence harvester.
#
# Runs the split benchmarks N times (default 5) in release mode and emits:
#
#   p0-results.jsonl              write-path components (p0_p2_bench) — one record per rep
#   p2-standalone-results.jsonl   embedded + loopback point query, default build
#   p2-full-feature-results.jsonl loopback point query, cluster,oidc,vault-kms
#   p0p2-threshold-verdict.jsonl  machine-enforced multi-rep median thresholds
#
# Every record carries the environment envelope (sha, tree, toolchain, runner,
# CPU, filesystem, features, rep). After all reps, multi-rep medians are
# computed and checked against hard tripwires; failure exits nonzero.
#
# Usage: scripts/run-p0-p2-measurements.sh OUT_DIR [REPS]
set -euo pipefail

OUT_DIR="${1:?usage: scripts/run-p0-p2-measurements.sh OUT_DIR [REPS]}"
REPS="${2:-${P0P2_REPS:-5}}"
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

run_bench() {
    local out_file="$1" features="$2" log_name="$3" rep="$4"
    shift 4
    local log="$OUT_DIR/${log_name}-rep${rep}.log"
    echo "== $log_name rep=$rep (features: ${features:-default})"
    CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-14}" \
        cargo "+$TOOLCHAIN" test --release "$@" -- --nocapture 2>&1 | tee "$log" \
        | grep -E '^\{' \
        | jq -c --argjson env "$ENV_JSON" --arg features "${features:-default}" --argjson rep "$rep" \
            '. + {environment: ($env + {features: $features, rep: $rep})}' \
        >> "$OUT_DIR/$out_file"
    grep -q "test result: ok" "$log"
}

: > "$OUT_DIR/p0-results.jsonl"
: > "$OUT_DIR/p2-standalone-results.jsonl"
: > "$OUT_DIR/p2-full-feature-results.jsonl"

for rep in $(seq 1 "$REPS"); do
    echo "==== P0/P2 repetition $rep / $REPS ===="
    run_bench p0-results.jsonl "" p0_p2_bench "$rep" \
        -p mongreldb-core --test p0_p2_bench

    run_bench p2-standalone-results.jsonl "" warm_point_query "$rep" \
        -p mongreldb-core --test qualification warm_point_query_p95_baseline
    run_bench p2-standalone-results.jsonl "" loopback_point_query_standalone "$rep" \
        -p mongreldb-server --test scale_test loopback_point_query_p95_baseline

    run_bench p2-full-feature-results.jsonl "cluster,oidc,vault-kms" loopback_point_query_full_feature "$rep" \
        -p mongreldb-server --test scale_test --features cluster,oidc,vault-kms \
        loopback_point_query_p95_baseline
done

# Machine-enforced multi-rep medians (B468-10). Thresholds are tripwires with
# headroom above measured healthy values in BENCHMARKS.md (µs unless noted).
# Fail closed when fewer than REPS samples exist or any median exceeds the gate.
python3 - "$OUT_DIR" "$REPS" <<'PY'
import json, sys, statistics
from pathlib import Path

out = Path(sys.argv[1])
reps = int(sys.argv[2])
failed = []
verdicts = []

def load(name):
    path = out / name
    rows = []
    for line in path.read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows

def median_field(rows, path_keys, field="p95_us"):
    vals = []
    for r in rows:
        cur = r
        ok = True
        for k in path_keys:
            if not isinstance(cur, dict) or k not in cur:
                ok = False
                break
            cur = cur[k]
        if not ok:
            continue
        if isinstance(cur, dict) and field in cur:
            vals.append(float(cur[field]))
        elif isinstance(cur, (int, float)) and field is None:
            vals.append(float(cur))
    return vals

# P0: samples nested under samples.<component>.p95_us
p0 = load("p0-results.jsonl")
if len(p0) < reps:
    failed.append(f"p0-results has {len(p0)} records, need {reps}")

p0_gates = {
    # Generous tripwires vs BENCHMARKS.md healthy medians (µs).
    "put_steady_state_on_reused_table": 100.0,   # healthy ~5.5 p95
    "first_put_after_create": 500_000.0,         # creation-bound, ms-scale ok
    "commit_fsync": 50_000.0,                    # healthy ~6.5 ms
    "table_create_only": 500_000.0,
}

for component, max_p95 in p0_gates.items():
    vals = []
    for r in p0:
        samples = r.get("samples") or {}
        comp = samples.get(component) or {}
        if "p95_us" in comp:
            vals.append(float(comp["p95_us"]))
    if len(vals) < reps:
        failed.append(f"p0/{component}: only {len(vals)} p95 samples, need {reps}")
        verdicts.append({"test": f"p0p2::threshold::p0::{component}", "metric": {"status": "fail", "reason": "insufficient_samples", "n": len(vals), "required_reps": reps}, "unit": "verdict"})
        continue
    med = statistics.median(vals)
    ok = med <= max_p95
    verdicts.append({
        "test": f"p0p2::threshold::p0::{component}",
        "metric": {
            "status": "pass" if ok else "fail",
            "median_p95_us": med,
            "max_p95_us": max_p95,
            "reps": len(vals),
            "samples": vals,
        },
        "unit": "verdict",
    })
    if not ok:
        failed.append(f"p0/{component}: median p95 {med} > {max_p95}")

def extract_point_p95(row):
    """P2 benches nest latency under point_query_latency.p95_us."""
    pql = row.get("point_query_latency")
    if isinstance(pql, dict) and "p95_us" in pql:
        return float(pql["p95_us"])
    if "p95_us" in row:
        return float(row["p95_us"])
    metric = row.get("metric")
    if isinstance(metric, dict) and "p95_us" in metric:
        return float(metric["p95_us"])
    return None

p2s = load("p2-standalone-results.jsonl")
p2f = load("p2-full-feature-results.jsonl")

# Tripwire: 250ms client-observed point query (BENCHMARKS.md / p95_tripwire_ms).
P2_MAX_P95_US = 250_000.0
WARM_MAX_P95_US = 50.0  # embedded warm is sub-3µs healthy

warm_vals = []
loop_vals = []
for r in p2s:
    p95 = extract_point_p95(r)
    if p95 is None:
        continue
    test = str(r.get("test", ""))
    if "warm" in test:
        warm_vals.append(p95)
    elif "loopback" in test:
        loop_vals.append(p95)
    elif p95 < 100.0:
        warm_vals.append(p95)
    else:
        loop_vals.append(p95)

for label, vals, max_p95 in [
    ("p2_standalone_warm_point_query", warm_vals, WARM_MAX_P95_US),
    ("p2_standalone_loopback_point_query", loop_vals, P2_MAX_P95_US),
]:
    if len(vals) < reps:
        verdicts.append({"test": f"p0p2::threshold::{label}", "metric": {"status": "fail", "reason": "insufficient_samples", "n": len(vals), "required_reps": reps}, "unit": "verdict"})
        failed.append(f"{label}: only {len(vals)} samples, need {reps}")
        continue
    med = statistics.median(vals)
    ok = med <= max_p95
    verdicts.append({
        "test": f"p0p2::threshold::{label}",
        "metric": {"status": "pass" if ok else "fail", "median_p95_us": med, "max_p95_us": max_p95, "reps": len(vals), "samples": vals},
        "unit": "verdict",
    })
    if not ok:
        failed.append(f"{label}: median p95 {med} > {max_p95}")

ff_vals = []
for r in p2f:
    p95 = extract_point_p95(r)
    if p95 is not None:
        ff_vals.append(p95)
if len(ff_vals) < reps:
    failed.append(f"p2_full_feature_loopback: only {len(ff_vals)} samples, need {reps}")
    verdicts.append({"test": "p0p2::threshold::p2_full_feature_loopback", "metric": {"status": "fail", "reason": "insufficient_samples", "n": len(ff_vals), "required_reps": reps}, "unit": "verdict"})
else:
    med = statistics.median(ff_vals)
    ok = med <= P2_MAX_P95_US
    verdicts.append({
        "test": "p0p2::threshold::p2_full_feature_loopback",
        "metric": {"status": "pass" if ok else "fail", "median_p95_us": med, "max_p95_us": P2_MAX_P95_US, "reps": len(ff_vals), "samples": ff_vals},
        "unit": "verdict",
    })
    if not ok:
        failed.append(f"p2_full_feature_loopback: median p95 {med} > {P2_MAX_P95_US}")

# Overall reps completeness
verdicts.append({
    "test": "p0p2::threshold::repetition_count",
    "metric": {
        "status": "pass" if len(p0) >= reps else "fail",
        "p0_records": len(p0),
        "p2_standalone_records": len(p2s),
        "p2_full_feature_records": len(p2f),
        "required_reps": reps,
    },
    "unit": "verdict",
})
if len(p0) < reps or len(p2s) < reps or len(p2f) < reps:
    failed.append(f"insufficient reps: p0={len(p0)} p2s={len(p2s)} p2f={len(p2f)} need {reps}")

# R170-08: same-runner ratio gates vs published healthy baselines.
# Absolute tripwires above remain catastrophic/sanity gates; ratios catch
# original-magnitude regressions (≈10–15%) against BENCHMARKS.md baselines.
ratio_cfg_path = Path("docs/ai/p0p2-baseline-ratios.json")
if ratio_cfg_path.is_file():
    ratio_cfg = json.loads(ratio_cfg_path.read_text())
    baselines = ratio_cfg.get("baseline_median_p95_us") or {}
    max_ratios = ratio_cfg.get("maximum_ratio") or {}
    # Map absolute-gate verdicts already computed to ratio checks.
    measured = {}
    for v in verdicts:
        m = v.get("metric") or {}
        if m.get("status") == "pass" and "median_p95_us" in m:
            name = v["test"].split("::")[-1]
            # p0 components are last path segment after p0::
            if v["test"].startswith("p0p2::threshold::p0::"):
                name = v["test"].rsplit("::", 1)[-1]
            measured[name] = float(m["median_p95_us"])
    for key, base in baselines.items():
        if key not in max_ratios:
            continue
        if key not in measured:
            # Prefer short name matches
            short = key
            if short not in measured:
                continue
        cand = measured.get(key)
        if cand is None:
            continue
        limit = float(base) * float(max_ratios[key])
        ok = cand <= limit + 1e-9
        verdicts.append({
            "test": f"p0p2::threshold::ratio::{key}",
            "metric": {
                "status": "pass" if ok else "fail",
                "candidate_median_p95_us": cand,
                "baseline_median_p95_us": float(base),
                "maximum_ratio": float(max_ratios[key]),
                "limit_p95_us": limit,
                "baseline_sha": ratio_cfg.get("p0_baseline_sha") or ratio_cfg.get("p2_baseline_sha"),
            },
            "unit": "verdict",
        })
        if not ok:
            failed.append(
                f"ratio/{key}: candidate median {cand} > baseline {base} × {max_ratios[key]} = {limit}"
            )
else:
    failed.append("missing docs/ai/p0p2-baseline-ratios.json — ratio gates required (R170-08)")

verdict_path = out / "p0p2-threshold-verdict.jsonl"
with verdict_path.open("w") as f:
    for v in verdicts:
        f.write(json.dumps(v, separators=(",", ":")) + "\n")

if failed:
    print("P0/P2 THRESHOLD FAILURES:", file=sys.stderr)
    for x in failed:
        print(f"  - {x}", file=sys.stderr)
    sys.exit(1)
print(f"== wrote {verdict_path} ({len(verdicts)} verdicts); all thresholds passed over {reps} reps")
PY

echo "== wrote $OUT_DIR/{p0-results,p2-standalone-results,p2-full-feature-results,p0p2-threshold-verdict}.jsonl"
