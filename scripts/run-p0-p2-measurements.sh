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
import json, sys, statistics, os
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

# FF-10/11/12: ratio gates. Prefer same-runner baseline harvest in
# out/baseline/ (or P0P2_BASELINE_DIR). Missing surface measurements fail closed.
# Static reference medians from config are only used when ratio_mode is
# "static_reference" (never labeled same_runner).
ratio_cfg_path = Path("docs/ai/p0p2-baseline-ratios.json")
if not ratio_cfg_path.is_file():
    failed.append("missing docs/ai/p0p2-baseline-ratios.json — ratio gates required (FF-10)")
else:
    ratio_cfg = json.loads(ratio_cfg_path.read_text())
    ratio_mode = ratio_cfg.get("ratio_mode") or "same_runner"
    surfaces = ratio_cfg.get("surfaces") or {}
    # Collect candidate medians from absolute-gate verdicts already computed.
    measured = {}
    for v in verdicts:
        m = v.get("metric") or {}
        if m.get("status") in ("pass", "fail") and "median_p95_us" in m:
            name = v["test"].rsplit("::", 1)[-1]
            measured[name] = float(m["median_p95_us"])
    # Load same-runner baseline harvest if present.
    baseline_dir = Path(os.environ.get("P0P2_BASELINE_DIR", str(out / "baseline")))
    baseline_measured = {}
    def load_baseline_surface(name, path_keys):
        # optional helper unused — surfaces use static or baseline jsonl later
        pass
    # Map baseline files into medians when baseline_dir exists.
    if baseline_dir.is_dir():
        def medians_from(file, extractor):
            path = baseline_dir / file
            if not path.is_file():
                return {}
            rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
            return extractor(rows)
        def p0_extract(rows):
            outm = {}
            for component in ["put_steady_state_on_reused_table","first_put_after_create","commit_fsync","table_create_only"]:
                vals=[]
                for r in rows:
                    samples=r.get("samples") or {}
                    comp=samples.get(component) or {}
                    if "p95_us" in comp:
                        vals.append(float(comp["p95_us"]))
                if vals:
                    outm[component]=statistics.median(vals)
            return outm
        def p2_extract(rows, warm=True):
            warm_vals=[]; loop_vals=[]
            for r in rows:
                pql=r.get("point_query_latency") or {}
                if "p95_us" not in pql:
                    continue
                p95=float(pql["p95_us"])
                test=str(r.get("test",""))
                if "warm" in test or p95 < 100:
                    warm_vals.append(p95)
                else:
                    loop_vals.append(p95)
            outm={}
            if warm_vals: outm["p2_standalone_warm_point_query"]=statistics.median(warm_vals)
            if loop_vals: outm["p2_standalone_loopback_point_query"]=statistics.median(loop_vals)
            return outm
        baseline_measured.update(p0_extract(load("p0-results.jsonl") if False else []))
        # load from baseline dir files
        bp0 = baseline_dir / "p0-results.jsonl"
        if bp0.is_file():
            brows=[json.loads(l) for l in bp0.read_text().splitlines() if l.strip()]
            baseline_measured.update(p0_extract(brows))
        bp2s = baseline_dir / "p2-standalone-results.jsonl"
        if bp2s.is_file():
            brows=[json.loads(l) for l in bp2s.read_text().splitlines() if l.strip()]
            warm_vals=[]; loop_vals=[]
            for r in brows:
                pql=r.get("point_query_latency") or {}
                if "p95_us" not in pql: continue
                p95=float(pql["p95_us"]); test=str(r.get("test",""))
                if "warm" in test or p95 < 100: warm_vals.append(p95)
                else: loop_vals.append(p95)
            if warm_vals: baseline_measured["p2_standalone_warm_point_query"]=statistics.median(warm_vals)
            if loop_vals: baseline_measured["p2_standalone_loopback_point_query"]=statistics.median(loop_vals)
        bp2f = baseline_dir / "p2-full-feature-results.jsonl"
        if bp2f.is_file():
            brows=[json.loads(l) for l in bp2f.read_text().splitlines() if l.strip()]
            vals=[float((r.get("point_query_latency") or {}).get("p95_us")) for r in brows if (r.get("point_query_latency") or {}).get("p95_us") is not None]
            if vals: baseline_measured["p2_full_feature_loopback"]=statistics.median(vals)

    for key, cfg in surfaces.items():
        max_ratio = float(cfg.get("maximum_ratio") or 1.10)
        base_sha = cfg.get("baseline_sha")
        cand = measured.get(key)
        if cand is None:
            verdicts.append({
                "test": f"p0p2::threshold::ratio::{key}",
                "metric": {"status": "fail", "reason": "missing_candidate_surface", "surface": key},
                "unit": "verdict",
            })
            failed.append(f"ratio/{key}: missing candidate measurement (fail closed FF-12)")
            continue
        if ratio_mode == "same_runner":
            base = baseline_measured.get(key)
            if base is None:
                # Allow explicit static_reference only when env opts in for fixtures.
                if os.environ.get("P0P2_ALLOW_STATIC_REFERENCE") == "1":
                    base = cfg.get("static_reference_median_p95_us")
                    mode_label = "static_reference_opt_in"
                else:
                    verdicts.append({
                        "test": f"p0p2::threshold::ratio::{key}",
                        "metric": {
                            "status": "fail",
                            "reason": "missing_same_runner_baseline",
                            "surface": key,
                            "hint": "populate OUT_DIR/baseline/*.jsonl from baseline SHA on same runner",
                            "baseline_sha": base_sha,
                        },
                        "unit": "verdict",
                    })
                    failed.append(f"ratio/{key}: missing same-runner baseline (FF-10/12)")
                    continue
            else:
                mode_label = "same_runner"
        else:
            base = cfg.get("static_reference_median_p95_us")
            mode_label = "static_reference"
            if base is None:
                verdicts.append({
                    "test": f"p0p2::threshold::ratio::{key}",
                    "metric": {"status": "fail", "reason": "missing_static_reference", "surface": key},
                    "unit": "verdict",
                })
                failed.append(f"ratio/{key}: missing static reference")
                continue
        base = float(base)
        limit = base * max_ratio
        ok = cand <= limit + 1e-9
        verdicts.append({
            "test": f"p0p2::threshold::ratio::{key}",
            "metric": {
                "status": "pass" if ok else "fail",
                "candidate_median_p95_us": cand,
                "baseline_median_p95_us": base,
                "maximum_ratio": max_ratio,
                "limit_p95_us": limit,
                "baseline_sha": base_sha,
                "ratio_mode": mode_label,
            },
            "unit": "verdict",
        })
        if not ok:
            failed.append(f"ratio/{key}: candidate {cand} > baseline {base} × {max_ratio} = {limit}")


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
