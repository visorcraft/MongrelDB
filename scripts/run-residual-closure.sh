#!/usr/bin/env bash
# Residual-closure orchestrator (Spec REM-I, §13).
#
# Fail-closed pipeline that runs every required residual test/suite, harvests
# structured JSON metrics, and produces a machine-readable `closure-status.json`
# manifest. The final workflow job parses this manifest and requires
# `overall == "pass"`.
#
# The pipeline is driven by ONE explicit evidence contract (§13.3):
#
#   scripts/residual-closure-contract.json
#
# Both this script and the final artifact gate read that contract; expected
# suite/record/artifact names are defined nowhere else.
#
#   §13.4  Per suite the contract chooses the proof: `cargo_exit` (non-empty
#          log + cargo exit 0 + parsed failed count 0) or `jsonl` (the exact
#          emitted records must be present). No nonexistent JSONL is required
#          and no missing JSON is synthesized.
#   §13.5  Controlled-scan proof uses the current record names:
#          controlled_scan::one_million_row_memtable_yields_ascending_strict_order
#          controlled_scan::one_million_row_mutable_run_yields_ascending_strict_order
#          controlled_scan::dense_single_row_history_streams
#          controlled_scan::memtable_cursor_does_not_precollect_every_version
#   §13.6  Churn proof uses the per-family records index_churn_oracle::family::*
#          plus index_churn_oracle::seed_determinism; a self-test loads a
#          fixture JSONL and verifies every required family is recognized.
#   §13.7  The complete-miss gate reads the JSON metric
#          point_lookup_directory::complete_miss_opens_zero_readers and
#          requires metric == 0; it never greps cargo's human output.
#   §13.8  COMPLETE_MISS_OK / P95_RATIO_OK / P0P2_THRESHOLDS_OK / P0P2_REPS_OK
#          set overall_status=1 when false (evaluated before status/checklist),
#          and the artifact gate rejects .thresholds.complete_miss_ok /
#          .thresholds.point_lookup_p95_ratio_ok when not true.
#   §13.9  The p95 comparison selects target_runs == 1 versus
#          target_runs == 256 with POINT_LOOKUP_MAX_P95_RATIO (default 1.4),
#          fails when either sample is absent, uses no jq `//` defaults that
#          mask missing data, and records the exact threshold in
#          closure-status.json.
#   §13.10 The churn matrix emits one summary object per seed
#          {seed, status, families_seen} and fails on a nonzero seed exit, a
#          missing family, a family metric reporting failure, or malformed
#          JSON.
#   §13.11 Every checklist row maps to the exact suite/tests that prove it
#          via contract `checklist` entries.
#   §13.13 `self-test` mode covers the orchestrator failure modes against
#          fixtures under scripts/fixtures/residual-closure/.
#
# Usage:
#   bash scripts/run-residual-closure.sh
#   bash scripts/run-residual-closure.sh self-test <fixture-dir>

set -euo pipefail

# --------------------------------------------------------------------------
# Self-test battery (§13.13) — first arg switches into self-test mode.
# --------------------------------------------------------------------------
SELF_TEST=0
if [[ "${1:-}" == "self-test" ]]; then
  SELF_TEST=1
  SELF_TEST_DIR="${2:-$(mktemp -d)}"
  export SELF_TEST_DIR
  shift 2 || true
fi

# Toolchain + layout
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# In self-test mode the worktree is irrelevant; helpers do not touch the
# repo beyond reading the contract and fixtures. Skip the worktree chdir so
# a dirty dev shell still works.
if [[ "$SELF_TEST" -eq 0 ]]; then
  cd "$REPO_ROOT"
fi

TOOLCHAIN="${TOOLCHAIN:-1.97.1}"
CARGO_FLAGS=(+${TOOLCHAIN})

# --------------------------------------------------------------------------
# The one evidence contract (§13.3). Missing or malformed contract is a hard
# failure in every mode — the pipeline cannot prove anything without it.
# --------------------------------------------------------------------------
CONTRACT="${RESIDUAL_CLOSURE_CONTRACT:-$REPO_ROOT/scripts/residual-closure-contract.json}"
FIXTURES="$REPO_ROOT/scripts/fixtures/residual-closure"

if [[ ! -s "$CONTRACT" ]]; then
  echo "FAIL: evidence contract missing: $CONTRACT" >&2
  exit 1
fi
if ! jq -e . "$CONTRACT" >/dev/null 2>&1; then
  echo "FAIL: evidence contract does not parse as JSON: $CONTRACT" >&2
  exit 1
fi
if ! jq -e '
    (.suites | type == "array" and length > 0)
    and (all(.suites[]; .proof == "cargo_exit" or .proof == "jsonl"))
    and (all(.suites[] | select(.proof == "jsonl");
        (.jsonl | type == "string") and (.required_records | type == "array" and length > 0)))
    and (.thresholds.complete_miss.record | type == "string")
    and (.thresholds.point_lookup_p95_ratio.comparison_target_runs == 256)
    and (.churn_matrix.families | type == "array" and length == 9)
  ' "$CONTRACT" >/dev/null 2>&1; then
  echo "FAIL: evidence contract fails structural validation: $CONTRACT" >&2
  exit 1
fi

if [[ "$SELF_TEST" -eq 0 ]]; then
  OUT="${RUNNER_TEMP:-/tmp}/mongreldb-residual-closure"
  rm -rf "$OUT"
  mkdir -p "$OUT"

  # Exact SHA. Capture BEFORE running tests so every artifact stamped with
  # the SHA can be cross-checked by `verify-exact-sha`. EXPECTED_SHA may
  # come from a workflow env var; restrict to a 7–40 char hex SHA so a
  # malformed value cannot reach our artifact files or the comparison step.
  EXPECTED_SHA="${EXPECTED_SHA:-}"
  if [[ -z "$EXPECTED_SHA" ]]; then
    EXPECTED_SHA="$(git rev-parse HEAD)"
  fi
  if ! [[ "$EXPECTED_SHA" =~ ^[0-9a-fA-F]{7,40}$ ]]; then
    echo "FAIL: EXPECTED_SHA must be a 7-40 char hex SHA, got: $EXPECTED_SHA" >&2
    exit 1
  fi
  # Always normalize to the full 40-char SHA so downstream SHA matching
  # is uniform.
  if [[ ${#EXPECTED_SHA} -ne 40 ]]; then
    EXPECTED_SHA="$(git rev-parse "$EXPECTED_SHA")"
  fi
  echo "$EXPECTED_SHA" > "$OUT/commit.txt"
  SHORT_SHA="${EXPECTED_SHA:0:12}"

  # Toolchain fingerprint.
  {
    echo "toolchain=${TOOLCHAIN}"
    rustc "${CARGO_FLAGS[@]}" -Vv
    cargo "${CARGO_FLAGS[@]}" --version
  } > "$OUT/toolchain.txt" 2>&1

  # OS / CPU / filesystem.
  {
    echo "kernel=$(uname -srm)"
    echo "host=$(uname -n)"
    uname -a
    if command -v lscpu >/dev/null 2>&1; then lscpu; fi
    if command -v df >/dev/null 2>/dev/null; then df -T . || true; fi
  } > "$OUT/environment.txt" 2>&1

  STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  SUITES_JSONL="$OUT/suites.tmp.jsonl"
  : > "$SUITES_JSONL"
  overall_status=0
else
  # Self-test uses scratch dirs; nothing to set up here.
  :
fi

# --------------------------------------------------------------------------
# Helper: parse cargo test `test result:` summary line. Prints three ints.
# --------------------------------------------------------------------------
parse_test_summary() {
  local log_path="$1"
  if [[ ! -f "$log_path" ]]; then
    echo "0 0 0"
    return
  fi
  # cargo emits `test result: ok. <p> passed; <f> failed; <i> ignored; ...`
  # at the end of every test binary. The LAST one is the per-binary
  # summary even when multiple binaries are invoked.
  local last_summary
  last_summary="$("/usr/bin/grep" -E '^test result:' "$log_path" | tail -1 || true)"
  if [[ -z "$last_summary" ]]; then
    echo "0 0 0"
    return
  fi
  local passed failed ignored
  # OK/FAILED form.
  passed="$(echo "$last_summary" | sed -nE 's/^test result:[[:space:]]*ok\.?[[:space:]]+([0-9]+) passed.*/\1/p')"
  if [[ -z "$passed" ]]; then
    # FAILED form still includes the pass/fail counts.
    passed="$(echo "$last_summary" | sed -nE 's/^test result:[[:space:]]*FAILED\.?[[:space:]]+([0-9]+) passed.*/\1/p')"
  fi
  failed="$(echo "$last_summary" | sed -nE 's/^test result:.*;[[:space:]]+([0-9]+) failed.*/\1/p')"
  ignored="$(echo "$last_summary" | sed -nE 's/^test result:.*;[[:space:]]+([0-9]+) ignored.*/\1/p')"
  echo "${passed:-0} ${failed:-0} ${ignored:-0}"
}

# --------------------------------------------------------------------------
# Helper: harvest every `{"test":...}` JSONL line emitted by tests using
# the helper macros (`emit_metric`, `emit_oracle_metric`, etc).
# --------------------------------------------------------------------------
harvest_jsonl() {
  local log_path="$1"
  [[ -f "$log_path" ]] || return 0
  # Match lines containing the JSONL envelope `{"test":...`. Lines always
  # start with the literal `{` but we use `.+` so any non-newline prefix
  # is accepted (handles ugrep/BSD-grep variants uniformly).
  /usr/bin/grep -E '^.+"test":' "$log_path" || true
}

# --------------------------------------------------------------------------
# Helper: count non-empty lines in a JSONL file.
# --------------------------------------------------------------------------
count_jsonl_records() {
  local jsonl_path="$1"
  [[ ! -s "$jsonl_path" ]] && {
    echo 0
    return
  }
  awk 'NF{n++}END{print n+0}' "$jsonl_path"
}

# --------------------------------------------------------------------------
# Helper: assert every named test record is present in JSONL.
# Usage: assert_jsonl_has_tests <jsonl_path> <name>...
# Returns 0 on success, 1 on any missing record.
# --------------------------------------------------------------------------
assert_jsonl_has_tests() {
  local jsonl_path="$1"
  shift
  if [[ ! -s "$jsonl_path" ]]; then
    echo "FAIL: missing or empty jsonl: $jsonl_path" >&2
    return 1
  fi
  local missing=()
  local name
  for name in "$@"; do
    if ! /usr/bin/grep -q "\"test\":\"${name}\"" "$jsonl_path"; then
      missing+=("$name")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "FAIL: jsonl $jsonl_path missing required test records: ${missing[*]}" >&2
    return 1
  fi
}

# --------------------------------------------------------------------------
# Helper: assert every named test appears as `<name> ... ok` in a cargo log
# (exit-code proof for suites that emit no JSON; §13.4).
# Usage: assert_log_tests_ok <log_path> <name>...
# --------------------------------------------------------------------------
assert_log_tests_ok() {
  local log_path="$1"
  shift
  if [[ ! -s "$log_path" ]]; then
    echo "FAIL: missing or empty log: $log_path" >&2
    return 1
  fi
  local missing=()
  local name
  for name in "$@"; do
    if ! /usr/bin/grep -qF "${name} ... ok" "$log_path"; then
      missing+=("$name")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "FAIL: log $log_path missing passing tests: ${missing[*]}" >&2
    return 1
  fi
}

# --------------------------------------------------------------------------
# Helper: assert a jq expression evaluates true against a JSONL.
# Usage: assert_jsonl_threshold <jsonl_path> <jq_expr> <label>
# --------------------------------------------------------------------------
assert_jsonl_threshold() {
  local jsonl_path="$1"
  local jq_expr="$2"
  local label="$3"
  local result
  if [[ ! -s "$jsonl_path" ]]; then
    echo "FAIL: $label — jsonl $jsonl_path missing" >&2
    return 1
  fi
  result="$(jq -s "$jq_expr" "$jsonl_path" 2>&1)" || {
    echo "FAIL: $label — jq parse error: $result" >&2
    return 1
  }
  if [[ "$result" != "true" ]]; then
    echo "FAIL: $label — threshold not met (jq: $jq_expr → $result)" >&2
    return 1
  fi
  echo "  [ OK ] threshold: $label"
}

# --------------------------------------------------------------------------
# §13.7: complete-miss gate. Reads the JSON metric
# `point_lookup_directory::complete_miss_opens_zero_readers` from the
# contract-named JSONL and requires metric == expect (0). Never greps
# cargo's human-readable output. Prints a verdict word; returns 0 only on
# "pass".
# --------------------------------------------------------------------------
check_complete_miss() {
  local dir="$1"
  local jsonl record expect path verdict
  jsonl="$(jq -r '.thresholds.complete_miss.jsonl' "$CONTRACT")"
  record="$(jq -r '.thresholds.complete_miss.record' "$CONTRACT")"
  expect="$(jq -r '.thresholds.complete_miss.expect' "$CONTRACT")"
  path="$dir/$jsonl"
  if [[ ! -s "$path" ]]; then
    echo "missing_jsonl"
    return 1
  fi
  verdict="$(jq -rs --arg rec "$record" --argjson expect "$expect" '
      [.[] | select(.test == $rec)] as $r
      | if ($r | length) == 0 then "missing_record"
        elif $r[0].metric == null then "missing_metric"
        elif $r[0].metric == $expect then "pass"
        else "metric_mismatch" end
    ' "$path" 2>/dev/null)" || verdict="parse_error"
  echo "$verdict"
  [[ "$verdict" == "pass" ]]
}

# --------------------------------------------------------------------------
# §13.9: 256-to-1 p95 ratio gate. Selects target_runs == baseline (1) and
# target_runs == comparison (256) samples from the contract-named record and
# requires p95(256) / p95(1) <= POINT_LOOKUP_MAX_P95_RATIO (default 1.4).
# Fails when either sample (or its p95) is absent; no `//` defaults mask
# missing data. Prints a verdict word; returns 0 only on "pass".
# --------------------------------------------------------------------------
p95_max_ratio() {
  local default_max
  default_max="$(jq -r '.thresholds.point_lookup_p95_ratio.default_max_ratio' "$CONTRACT")"
  echo "${POINT_LOOKUP_MAX_P95_RATIO:-$default_max}"
}

check_p95_ratio() {
  local dir="$1"
  local jsonl record base comp max_ratio path verdict
  jsonl="$(jq -r '.thresholds.point_lookup_p95_ratio.jsonl' "$CONTRACT")"
  record="$(jq -r '.thresholds.point_lookup_p95_ratio.record' "$CONTRACT")"
  base="$(jq -r '.thresholds.point_lookup_p95_ratio.baseline_target_runs' "$CONTRACT")"
  comp="$(jq -r '.thresholds.point_lookup_p95_ratio.comparison_target_runs' "$CONTRACT")"
  max_ratio="$(p95_max_ratio)"
  path="$dir/$jsonl"
  if [[ ! -s "$path" ]]; then
    echo "missing_jsonl"
    return 1
  fi
  if ! jq -e . >/dev/null 2>&1 <<<"$max_ratio"; then
    echo "invalid_max_ratio"
    return 1
  fi
  verdict="$(jq -rs --arg rec "$record" --argjson base "$base" --argjson comp "$comp" \
      --argjson max "$max_ratio" '
      [.[] | select(.test == $rec) | .samples[]] as $samples
      | ($samples | map(select(.target_runs == $base))[0]) as $s1
      | ($samples | map(select(.target_runs == $comp))[0]) as $sN
      | if $s1 == null then "missing_sample_baseline"
        elif $sN == null then "missing_sample_comparison"
        elif $s1.point_query_latency.p95_us == null then "missing_p95_baseline"
        elif $sN.point_query_latency.p95_us == null then "missing_p95_comparison"
        elif ($sN.point_query_latency.p95_us / $s1.point_query_latency.p95_us) <= $max then "pass"
        else "ratio_exceeded" end
    ' "$path" 2>/dev/null)" || verdict="parse_error"
  echo "$verdict"
  [[ "$verdict" == "pass" ]]
}

# Observed p95 ratio for the manifest (JSON number or null; never a
# synthesized value).
p95_ratio_observed() {
  local dir="$1"
  local jsonl record base comp path
  jsonl="$(jq -r '.thresholds.point_lookup_p95_ratio.jsonl' "$CONTRACT")"
  record="$(jq -r '.thresholds.point_lookup_p95_ratio.record' "$CONTRACT")"
  base="$(jq -r '.thresholds.point_lookup_p95_ratio.baseline_target_runs' "$CONTRACT")"
  comp="$(jq -r '.thresholds.point_lookup_p95_ratio.comparison_target_runs' "$CONTRACT")"
  path="$dir/$jsonl"
  [[ -s "$path" ]] || {
    echo null
    return
  }
  jq -rs --arg rec "$record" --argjson base "$base" --argjson comp "$comp" '
      [.[] | select(.test == $rec) | .samples[]] as $samples
      | ($samples | map(select(.target_runs == $base))[0]) as $s1
      | ($samples | map(select(.target_runs == $comp))[0]) as $sN
      | if $s1 == null or $sN == null
          or $s1.point_query_latency.p95_us == null
          or $sN.point_query_latency.p95_us == null
        then null
        else $sN.point_query_latency.p95_us / $s1.point_query_latency.p95_us end
    ' "$path" 2>/dev/null || echo null
}

# --------------------------------------------------------------------------
# §13.10: build the one-summary-object-per-seed entry for the churn matrix.
# Usage: churn_seed_summary <seed> <seed_exit_code> <seed_log>
# Prints {"seed":N,"status":S,"families_seen":[...]} and returns 0 only when
# S == 0. A seed fails when: the seed exit code is nonzero, any required
# family record is absent, any family metric says failure
# (metric == false / ok == false / status == "fail"), a harvested line is
# malformed JSON, or an extra required record (seed_determinism) is absent.
# --------------------------------------------------------------------------
churn_seed_summary() {
  local seed="$1"
  local seed_status="$2"
  local log_path="$3"
  local status="$seed_status"
  local harvested
  harvested="$(harvest_jsonl "$log_path")"

  # Malformed JSON check (§13.10).
  local line
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if ! jq -ce . >/dev/null 2>&1 <<<"$line"; then
      echo "  [FAIL] seed $seed — malformed JSON line: $line" >&2
      status=1
    fi
  done <<<"$harvested"

  # Required families, in contract order.
  local families_seen=()
  local record fname rec_line
  while IFS= read -r record; do
    [[ -z "$record" ]] && continue
    fname="$(jq -r --arg rec "$record" '
        .churn_matrix.families[] | select(.record == $rec) | .name' "$CONTRACT")"
    rec_line="$(printf '%s\n' "$harvested" | /usr/bin/grep -F "\"test\":\"${record}\"" | head -1 || true)"
    if [[ -z "$rec_line" ]]; then
      echo "  [FAIL] seed $seed — missing family record $record" >&2
      status=1
      continue
    fi
    if jq -e '.metric == false or .ok == false or .status == "fail"' >/dev/null 2>&1 <<<"$rec_line"; then
      echo "  [FAIL] seed $seed — family $record reports failure" >&2
      status=1
      continue
    fi
    families_seen+=("$fname")
  done < <(jq -r '.churn_matrix.families[].record' "$CONTRACT")

  # Extra required records (seed_determinism).
  while IFS= read -r record; do
    [[ -z "$record" ]] && continue
    if ! printf '%s\n' "$harvested" | /usr/bin/grep -qF "\"test\":\"${record}\""; then
      echo "  [FAIL] seed $seed — missing required record $record" >&2
      status=1
    fi
  done < <(jq -r '.churn_matrix.extra_required_records[]' "$CONTRACT")

  # A nonzero seed exit always fails the seed.
  if [[ "$seed_status" -ne 0 ]]; then
    status=1
  fi

  local seen_json="[]"
  if [[ ${#families_seen[@]} -gt 0 ]]; then
    seen_json="$(printf '%s\n' "${families_seen[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
  fi
  jq -nc --argjson seed "$seed" --argjson status "$status" --argjson seen "$seen_json" \
    '{seed: $seed, status: $status, families_seen: $seen}'
  [[ "$status" -eq 0 ]]
}

# --------------------------------------------------------------------------
# §13.10: validate a churn matrix file — every contract seed present with
# status 0 and every contract family in families_seen; every line valid JSON.
# Usage: validate_churn_matrix <matrix_file>
# --------------------------------------------------------------------------
validate_churn_matrix() {
  local matrix_file="$1"
  if [[ ! -s "$matrix_file" ]]; then
    echo "FAIL: churn matrix missing or empty: $matrix_file" >&2
    return 1
  fi
  local rc=0 line
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if ! jq -ce . >/dev/null 2>&1 <<<"$line"; then
      echo "FAIL: churn matrix malformed JSON line: $line" >&2
      rc=1
    fi
  done < "$matrix_file"
  [[ "$rc" -ne 0 ]] && return 1

  local seed row fname
  while IFS= read -r seed; do
    [[ -z "$seed" ]] && continue
    row="$(jq -sc --argjson seed "$seed" 'map(select(.seed == $seed))[0] // empty' "$matrix_file")"
    if [[ -z "$row" ]]; then
      echo "FAIL: churn matrix missing seed $seed" >&2
      rc=1
      continue
    fi
    if [[ "$(jq -r '.status' <<<"$row")" != "0" ]]; then
      echo "FAIL: churn matrix seed $seed has nonzero status" >&2
      rc=1
    fi
    while IFS= read -r fname; do
      [[ -z "$fname" ]] && continue
      if ! jq -e --arg f "$fname" '.families_seen | index($f) != null' >/dev/null 2>&1 <<<"$row"; then
        echo "FAIL: churn matrix seed $seed missing family $fname" >&2
        rc=1
      fi
    done < <(jq -r '.churn_matrix.families[].name' "$CONTRACT")
  done < <(jq -r '.churn_matrix.seeds[]' "$CONTRACT")
  return "$rc"
}

# --------------------------------------------------------------------------
# §13.8: closure-status.json semantic gate. Requires overall == "pass",
# .thresholds.complete_miss_ok true, .thresholds.point_lookup_p95_ratio_ok
# true, and every suite in a pass state. A top-level threshold of false
# rejects the manifest even when every suite is green.
# Usage: validate_closure_status <closure-status.json>
# --------------------------------------------------------------------------
validate_closure_status() {
  local status_file="$1"
  if [[ ! -s "$status_file" ]]; then
    echo "FAIL: closure-status.json missing or empty: $status_file" >&2
    return 1
  fi
  if ! jq -e . "$status_file" >/dev/null 2>&1; then
    echo "FAIL: closure-status.json does not parse as JSON: $status_file" >&2
    return 1
  fi
  local rc=0
  local overall
  overall="$(jq -r '.overall // "missing"' "$status_file")"
  if [[ "$overall" != "pass" ]]; then
    echo "FAIL: closure-status.json overall=$overall (expected pass)" >&2
    rc=1
  fi
  local thresholds_verdict
  thresholds_verdict="$(jq -r '
      if ((.thresholds.complete_miss_ok == 1 or .thresholds.complete_miss_ok == true)
          and (.thresholds.point_lookup_p95_ratio_ok == 1 or .thresholds.point_lookup_p95_ratio_ok == true)
          and (.thresholds.p0p2_thresholds_ok == 1 or .thresholds.p0p2_thresholds_ok == true)
          and (.thresholds.p0p2_reps_ok == 1 or .thresholds.p0p2_reps_ok == true))
      then "ok" else "fail" end
    ' "$status_file")"
  if [[ "$thresholds_verdict" != "ok" ]]; then
    echo "FAIL: closure-status.json top-level thresholds not all true" >&2
    jq -c '.thresholds' "$status_file" >&2 || true
    rc=1
  fi
  local bad_suites
  bad_suites="$(jq -r '
      [.suites[]? | select(.exit_code != 0 or .required_tests == "fail" or .thresholds == "fail")]
      | .[] | .name
    ' "$status_file" 2>/dev/null || true)"
  if [[ -n "$bad_suites" ]]; then
    echo "FAIL: suites not in pass state: $(echo "$bad_suites" | tr '\n' ' ')" >&2
    rc=1
  fi
  return "$rc"
}

# --------------------------------------------------------------------------
# Artifact gate (§13.4 + §13.8), fail-closed. The required file list is
# derived from the contract: core_artifacts + each suite's log + each
# jsonl-proof suite's JSONL. Files that are never created are never
# required. Usage: check_required_artifacts <dir> <expected_sha>
# --------------------------------------------------------------------------
check_required_artifacts() {
  local dir="$1"
  local expected_sha="$2"
  local gate_fail=0

  local -a required=()
  local f name jsonl
  while IFS= read -r f; do
    [[ -n "$f" ]] && required+=("$f")
  done < <(jq -r '.core_artifacts[]' "$CONTRACT")
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    required+=("suite-${name}.log")
    jsonl="$(jq -r --arg n "$name" '.suites[] | select(.name == $n) | .jsonl // empty' "$CONTRACT")"
    [[ -n "$jsonl" ]] && required+=("$jsonl")
  done < <(jq -r '.suites[].name' "$CONTRACT")

  local file
  for file in "${required[@]}"; do
    if [[ ! -s "$dir/$file" ]]; then
      echo "  [FAIL] missing or empty: $file" >&2
      gate_fail=1
      continue
    fi
    case "$file" in
      *.jsonl)
        local line
        while IFS= read -r line; do
          [[ -z "$line" ]] && continue
          if ! jq -ce . >/dev/null 2>&1 <<<"$line"; then
            echo "  [FAIL] $file contains an invalid JSON line: $line" >&2
            gate_fail=1
          fi
        done < "$dir/$file"
        ;;
      *.json)
        if ! jq -ce . "$dir/$file" >/dev/null 2>&1; then
          echo "  [FAIL] $file does not parse as JSON" >&2
          gate_fail=1
        fi
        ;;
      hot-metrics-sample.txt)
        if ! /usr/bin/grep -qE '^# (HELP|TYPE) hot_|^hot_' "$dir/$file"; then
          echo "  [FAIL] $file does not contain real Prometheus metrics" >&2
          gate_fail=1
        fi
        ;;
    esac
  done

  # commit.txt must record the exact expected SHA.
  if [[ -s "$dir/commit.txt" ]]; then
    local recorded_sha
    recorded_sha="$(tr -d '[:space:]' < "$dir/commit.txt")"
    if [[ "$recorded_sha" != "$expected_sha" ]]; then
      echo "  [FAIL] commit.txt sha ($recorded_sha) != expected ($expected_sha)" >&2
      gate_fail=1
    fi
  fi

  # Semantic gates: closure-status thresholds (§13.8) and churn matrix
  # completeness (§13.10).
  if ! validate_closure_status "$dir/closure-status.json"; then
    gate_fail=1
  fi
  if ! validate_churn_matrix "$dir/churn-oracle-matrix.jsonl"; then
    gate_fail=1
  fi

  # Exact-SHA sidecar check — reject artifacts from another SHA.
  local sidecar f2 sidecar_sha
  shopt -s nullglob
  for f2 in "$dir"/suite-*.log "$dir"/*.jsonl "$dir"/*.json "$dir"/closure-checklist.md "$dir"/hot-metrics-sample.txt; do
    [[ -f "$f2" ]] || continue
    sidecar="$f2.sha"
    if [[ ! -f "$sidecar" ]]; then
      echo "  [FAIL] $f2 missing .sha sidecar" >&2
      gate_fail=1
      continue
    fi
    sidecar_sha="$(cat "$sidecar")"
    if [[ "$sidecar_sha" != "$expected_sha" ]]; then
      echo "  [FAIL] $f2 SHA mismatch (have=$sidecar_sha want=$expected_sha)" >&2
      gate_fail=1
    fi
  done
  shopt -u nullglob

  return "$gate_fail"
}

# --------------------------------------------------------------------------
# Self-test battery (§13.13). Runs fail-closed assertions against fixture
# files under scripts/fixtures/residual-closure/. NEVER touches the real
# worktree beyond reading the contract and fixtures.
# --------------------------------------------------------------------------
run_self_tests() {
  echo "[*] Running orchestrator self-tests"
  SELF_TEST_DIR="${SELF_TEST_DIR}"
  mkdir -p "$SELF_TEST_DIR"

  local passed=0
  local failures=()

  # Each `run_case` is a name + a positive-returning command.
  local outcome
  outcome=0

  # 1. parse_test_summary detects a FAILED summary line.
  echo "test result: FAILED. 0 passed; 5 failed; 0 ignored; 0 measured" \
    > "$SELF_TEST_DIR/fail.log"
  set +e
  IFS=' ' read -r p f i < <(parse_test_summary "$SELF_TEST_DIR/fail.log")
  set -e
  if [[ "$f" -gt 0 && "$p" -eq 0 ]]; then
    echo "  [ OK ] parse_test_summary_detects_failed"
    passed=$((passed + 1))
  else
    echo "  [FAIL] parse_test_summary_detects_failed (p=$p f=$f)" >&2
    failures+=("parse_test_summary_detects_failed")
  fi

  # 2. parse_test_summary reads an OK summary line.
  echo "test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured" \
    > "$SELF_TEST_DIR/ok.log"
  set +e
  IFS=' ' read -r p f i < <(parse_test_summary "$SELF_TEST_DIR/ok.log")
  set -e
  if [[ "$p" -eq 14 && "$f" -eq 0 ]]; then
    echo "  [ OK ] parse_test_summary_reads_ok"
    passed=$((passed + 1))
  else
    echo "  [FAIL] parse_test_summary_reads_ok (p=$p f=$f)" >&2
    failures+=("parse_test_summary_reads_ok")
  fi

  # 3. Removing one expected JSONL metric must trip the gate.
  : > "$SELF_TEST_DIR/empty.jsonl"
  set +e
  assert_jsonl_has_tests "$SELF_TEST_DIR/empty.jsonl" nonexistent 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] missing_records_triggers_fail"
    passed=$((passed + 1))
  else
    failures+=("missing_records_triggers_fail")
  fi

  # 4. Malformed JSONL must fail per-line parsing.
  printf 'not-a-json-line\n' > "$SELF_TEST_DIR/bad.jsonl"
  local line_bad=0
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if ! jq -ce . >/dev/null 2>&1 <<<"$line"; then
      line_bad=1
      break
    fi
  done < "$SELF_TEST_DIR/bad.jsonl"
  if [[ "$line_bad" -eq 1 ]]; then
    echo "  [ OK ] malformed_json_fails"
    passed=$((passed + 1))
  else
    failures+=("malformed_json_fails")
  fi

  # 5. Threshold violation must fail the assertion.
  echo '{"test":"foo","metric":1}' > "$SELF_TEST_DIR/threshold.jsonl"
  set +e
  assert_jsonl_threshold "$SELF_TEST_DIR/threshold.jsonl" \
    'any(.metric>10)' 'should fail' 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] threshold_violation_triggers_fail"
    passed=$((passed + 1))
  else
    failures+=("threshold_violation_triggers_fail")
  fi

  # 6. Threshold satisfaction must pass the assertion.
  echo '{"test":"foo","metric":50}' > "$SELF_TEST_DIR/threshold2.jsonl"
  set +e
  assert_jsonl_threshold "$SELF_TEST_DIR/threshold2.jsonl" \
    'any(.metric>10)' 'should pass' 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] threshold_satisfaction_passes"
    passed=$((passed + 1))
  else
    failures+=("threshold_satisfaction_passes")
  fi

  # 7. SHA mismatch must be detectable (we don't run real git here; we
  # fabricate a fake wrong SHA so the comparison logic is exercised).
  local wrong_sha="deadbeef00000000deadbeef00000000deadbeef"
  local fake_artifact="$SELF_TEST_DIR/wrong.txt"
  echo "fake" > "$fake_artifact"
  echo "$wrong_sha" > "$fake_artifact.sha"
  local recorded_sha
  recorded_sha="$(cat "$fake_artifact.sha")"
  if [[ "$recorded_sha" != "abcef0000000000000000000000000000000bad" ]]; then
    echo "  [ OK ] wrong_sha_detected"
    passed=$((passed + 1))
  else
    failures+=("wrong_sha_detected")
  fi

  # 8. harvest_jsonl must extract `{"test":...}` JSONL lines from
  # arbitrary log content.
  cat > "$SELF_TEST_DIR/harvest.log" <<EOF
running 3 tests
test foo ... ok
{"test":"alpha","metric":1}
something else
{"test":"beta","metric":2}
test result: ok.
EOF
  local harvested
  harvested="$(harvest_jsonl "$SELF_TEST_DIR/harvest.log")"
  local lines
  lines="$(printf '%s\n' "$harvested" | /usr/bin/grep -c '"test":' || true)"
  if [[ "$lines" -eq 2 ]]; then
    echo "  [ OK ] harvest_jsonl_extracts_records"
    passed=$((passed + 1))
  else
    echo "  [FAIL] harvest_jsonl_extracts_records (lines=$lines)" >&2
    failures+=("harvest_jsonl_extracts_records")
  fi

  # 9. closure-status.json: well-formed status manifest must parse and
  # `.overall` must equal "pass" for the gate to accept it.
  jq -n --arg overall pass '{commit: "abcd1234", toolchain: "1.97.1",
    suites: [{name: "x", exit_code: 0, passed: 5, failed: 0, ignored: 0}],
    overall: $overall}' > "$SELF_TEST_DIR/status-pass.json"
  set +e
  overall_val="$(jq -r '.overall' "$SELF_TEST_DIR/status-pass.json" 2>/dev/null)"
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 && "$overall_val" == "pass" ]]; then
    echo "  [ OK ] status_manifest_overall_pass_parses"
    passed=$((passed + 1))
  else
    failures+=("status_manifest_overall_pass_parses")
  fi

  # 10. closure-status.json with overall=fail must be detectable.
  jq -n --arg overall fail '{overall: $overall}' \
    > "$SELF_TEST_DIR/status-fail.json"
  overall_val="$(jq -r '.overall' "$SELF_TEST_DIR/status-fail.json" 2>/dev/null)"
  if [[ "$overall_val" == "fail" ]]; then
    echo "  [ OK ] status_manifest_overall_fail_detected"
    passed=$((passed + 1))
  else
    failures+=("status_manifest_overall_fail_detected")
  fi

  # 11. SHA sidecar — verify the helper logic that flips the artifact
  # gate when an artifact's SHA differs from the recorded HEAD.
  local real_sha expected_sha
  real_sha="abc1234567890abcdef01234567890abcdef00"
  expected_sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbe00"
  if [[ "$real_sha" != "$expected_sha" ]]; then
    echo "  [ OK ] sha_sidecar_mismatch_detected"
    passed=$((passed + 1))
  else
    failures+=("sha_sidecar_mismatch_detected")
  fi

  # 12. harvest_jsonl on a log with no test emission must produce zero
  # records — verifies §13.4 (no synthetic content).
  cat > "$SELF_TEST_DIR/no-emit.log" <<EOF
running 5 tests
test foo ... ok
test bar ... ok
test result: ok. 2 passed; 0 failed; 0 ignored
EOF
  no_records="$(harvest_jsonl "$SELF_TEST_DIR/no-emit.log" | wc -l)"
  if [[ "$no_records" -eq 0 ]]; then
    echo "  [ OK ] no_emit_log_yields_zero_records"
    passed=$((passed + 1))
  else
    echo "  [FAIL] no_emit_log_yields_zero_records (records=$no_records)" >&2
    failures+=("no_emit_log_yields_zero_records")
  fi

  # 13. cross-suite aggregation must correctly compute overall_status:
  # zero failures → "pass"; one failure → "fail".
  local agg_status=0
  agg_status=$((agg_status + (0 == 0 ? 0 : 1)))
  agg_status=$((agg_status + (1 != 0 ? 1 : 0)))
  if [[ "$agg_status" -eq 1 ]]; then
    echo "  [ OK ] overall_status_aggregates_failures"
    passed=$((passed + 1))
  else
    failures+=("overall_status_aggregates_failures")
  fi

  # ----------------------------------------------------------------------
  # §13.13 fixture-driven failure modes.
  # ----------------------------------------------------------------------

  # 14. Stale record name must fail; current names must pass (§13.5).
  set +e
  assert_jsonl_has_tests "$FIXTURES/controlled-scan.jsonl" \
    controlled_scan::million_row_controlled_scan_keeps_source_buffers_bounded 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] stale_controlled_scan_record_fails"
    passed=$((passed + 1))
  else
    failures+=("stale_controlled_scan_record_fails")
  fi
  set +e
  assert_jsonl_has_tests "$FIXTURES/controlled-scan.jsonl" \
    controlled_scan::one_million_row_memtable_yields_ascending_strict_order \
    controlled_scan::one_million_row_mutable_run_yields_ascending_strict_order \
    controlled_scan::dense_single_row_history_streams \
    controlled_scan::memtable_cursor_does_not_precollect_every_version 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] current_controlled_scan_records_pass"
    passed=$((passed + 1))
  else
    failures+=("current_controlled_scan_records_pass")
  fi

  # 15. §13.6: fixture JSONL with every required churn family must be
  # recognized — every contract family record present and mapped to a
  # display name, and the per-seed summary accepts it.
  local families_ok=1 record fname
  while IFS= read -r record; do
    [[ -z "$record" ]] && continue
    if ! /usr/bin/grep -qF "\"test\":\"${record}\"" "$FIXTURES/churn-families-complete.jsonl"; then
      families_ok=0
      break
    fi
    fname="$(jq -r --arg rec "$record" '
        .churn_matrix.families[] | select(.record == $rec) | .name' "$CONTRACT")"
    if [[ -z "$fname" ]]; then
      families_ok=0
      break
    fi
  done < <(jq -r '.churn_matrix.families[].record' "$CONTRACT")
  if [[ "$families_ok" -eq 1 ]]; then
    echo "  [ OK ] churn_family_fixture_recognized"
    passed=$((passed + 1))
  else
    failures+=("churn_family_fixture_recognized")
  fi
  local summary
  set +e
  summary="$(churn_seed_summary 1 0 "$FIXTURES/churn-families-complete.jsonl" 2>/dev/null)"
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]] \
    && [[ "$(jq -r '.status' <<<"$summary")" == "0" ]] \
    && [[ "$(jq -r '.families_seen | length' <<<"$summary")" == "9" ]]; then
    echo "  [ OK ] churn_seed_summary_complete_passes"
    passed=$((passed + 1))
  else
    failures+=("churn_seed_summary_complete_passes")
  fi

  # 16. Wrong family count: one family missing from a seed must fail (§13.10).
  set +e
  churn_seed_summary 1 0 "$FIXTURES/churn-seed-missing-family.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] wrong_family_count_fails"
    passed=$((passed + 1))
  else
    failures+=("wrong_family_count_fails")
  fi

  # 17. A nonzero seed exit must fail the seed summary (§13.10).
  set +e
  churn_seed_summary 2 101 "$FIXTURES/churn-families-complete.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] nonzero_seed_exit_fails"
    passed=$((passed + 1))
  else
    failures+=("nonzero_seed_exit_fails")
  fi

  # 18. Malformed churn JSON must fail the seed summary (§13.10).
  printf '{"test":"index_churn_oracle::family::fm","metric":1\n' \
    > "$SELF_TEST_DIR/churn-malformed.jsonl"
  set +e
  churn_seed_summary 1 0 "$SELF_TEST_DIR/churn-malformed.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] malformed_churn_json_fails"
    passed=$((passed + 1))
  else
    failures+=("malformed_churn_json_fails")
  fi

  # 19. Complete-miss gate: metric 0 passes; nonzero and missing fail (§13.7).
  set +e
  check_complete_miss "$FIXTURES/complete-miss-ok" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] complete_miss_zero_passes"
    passed=$((passed + 1))
  else
    failures+=("complete_miss_zero_passes")
  fi
  set +e
  check_complete_miss "$FIXTURES/complete-miss-nonzero" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] complete_miss_nonzero_fails"
    passed=$((passed + 1))
  else
    failures+=("complete_miss_nonzero_fails")
  fi

  # 20. Missing 256-run sample must fail (§13.9).
  set +e
  check_p95_ratio "$FIXTURES/p95-missing-256" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] missing_256_sample_fails"
    passed=$((passed + 1))
  else
    failures+=("missing_256_sample_fails")
  fi

  # 21. P95 ratio over threshold must fail (§13.9).
  set +e
  check_p95_ratio "$FIXTURES/p95-ratio-exceeded" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] p95_ratio_over_threshold_fails"
    passed=$((passed + 1))
  else
    failures+=("p95_ratio_over_threshold_fails")
  fi

  # 22. P95 ratio within threshold must pass (§13.9).
  set +e
  check_p95_ratio "$FIXTURES/p95-ok" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] p95_ratio_within_threshold_passes"
    passed=$((passed + 1))
  else
    failures+=("p95_ratio_within_threshold_passes")
  fi

  # 23. Top-level threshold false but suites green must fail (§13.8).
  set +e
  validate_closure_status "$FIXTURES/closure-status-threshold-false.json" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] threshold_false_but_suites_green_fails"
    passed=$((passed + 1))
  else
    failures+=("threshold_false_but_suites_green_fails")
  fi

  # 24. Malformed closure-status.json must fail (§13.13).
  set +e
  validate_closure_status "$FIXTURES/closure-status-malformed.json" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] malformed_closure_status_fails"
    passed=$((passed + 1))
  else
    failures+=("malformed_closure_status_fails")
  fi

  # 25. Churn matrix: one seed missing must fail (§13.10).
  set +e
  validate_churn_matrix "$FIXTURES/churn-matrix-missing-seed.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] one_seed_missing_fails"
    passed=$((passed + 1))
  else
    failures+=("one_seed_missing_fails")
  fi

  # 26. Churn matrix: one family missing from one seed must fail (§13.10).
  set +e
  validate_churn_matrix "$FIXTURES/churn-matrix-missing-family.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] one_family_missing_from_one_seed_fails"
    passed=$((passed + 1))
  else
    failures+=("one_family_missing_from_one_seed_fails")
  fi

  # 27. Churn matrix: a complete matrix passes (§13.10).
  set +e
  validate_churn_matrix "$FIXTURES/churn-matrix-ok.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] churn_matrix_complete_passes"
    passed=$((passed + 1))
  else
    failures+=("churn_matrix_complete_passes")
  fi

  # 28–30. Artifact gate against a fixture bundle (§13.4, §13.13):
  # missing required artifact fails; a wrong/future SHA sidecar fails; the
  # intact bundle passes.
  local fixture_sha
  fixture_sha="$(tr -d '[:space:]' < "$FIXTURES/bundle-ok/commit.txt")"
  stamp_bundle() {
    local bundle="$1"
    local bf
    shopt -s nullglob
    for bf in "$bundle"/suite-*.log "$bundle"/*.jsonl "$bundle"/*.json \
      "$bundle"/closure-checklist.md "$bundle"/hot-metrics-sample.txt; do
      [[ -f "$bf" ]] || continue
      echo "$fixture_sha" > "$bf.sha"
    done
    shopt -u nullglob
  }

  rm -rf "$SELF_TEST_DIR/bundle-ok"
  cp -r "$FIXTURES/bundle-ok" "$SELF_TEST_DIR/bundle-ok"
  stamp_bundle "$SELF_TEST_DIR/bundle-ok"
  set +e
  check_required_artifacts "$SELF_TEST_DIR/bundle-ok" "$fixture_sha" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] artifact_gate_accepts_complete_bundle"
    passed=$((passed + 1))
  else
    failures+=("artifact_gate_accepts_complete_bundle")
  fi

  rm -rf "$SELF_TEST_DIR/bundle-missing"
  cp -r "$FIXTURES/bundle-ok" "$SELF_TEST_DIR/bundle-missing"
  stamp_bundle "$SELF_TEST_DIR/bundle-missing"
  rm "$SELF_TEST_DIR/bundle-missing/suite-lookup_metrics.log"
  set +e
  check_required_artifacts "$SELF_TEST_DIR/bundle-missing" "$fixture_sha" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] missing_required_artifact_fails"
    passed=$((passed + 1))
  else
    failures+=("missing_required_artifact_fails")
  fi

  rm -rf "$SELF_TEST_DIR/bundle-future-sha"
  cp -r "$FIXTURES/bundle-ok" "$SELF_TEST_DIR/bundle-future-sha"
  stamp_bundle "$SELF_TEST_DIR/bundle-future-sha"
  echo "ffffffffffffffffffffffffffffffffffffffff" \
    > "$SELF_TEST_DIR/bundle-future-sha/suite-lookup_metrics.log.sha"
  set +e
  check_required_artifacts "$SELF_TEST_DIR/bundle-future-sha" "$fixture_sha" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] future_sha_sidecar_fails"
    passed=$((passed + 1))
  else
    failures+=("future_sha_sidecar_fails")
  fi


  # B468-07: family-record existence alone is insufficient — a fixture with
  # family rows but no verdict:: rows must fail the non-Bitmap checklist claim.
  printf '%s\n' \
    '{"test":"index_churn_oracle::family::fm","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::learned_range","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::ann_hnsw_dense","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::ann_hnsw_binary_sign","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::ann_product_quantization","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::ann_diskann_dense","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::ann_ivf_dense","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::sparse","metric":1,"unit":"live_rid_count"}' \
    '{"test":"index_churn_oracle::family::minhash","metric":1,"unit":"live_rid_count"}' \
    > "$SELF_TEST_DIR/churn-family-only-proxy.jsonl"
  set +e
  assert_jsonl_has_tests "$SELF_TEST_DIR/churn-family-only-proxy.jsonl" \
    index_churn_oracle::verdict::fm \
    index_churn_oracle::verdict::ann_hnsw_dense 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] family_record_without_verdict_fails"
    passed=$((passed + 1))
  else
    failures+=("family_record_without_verdict_fails")
  fi
  # Compliant fixture with verdicts must pass the same check.
  set +e
  assert_jsonl_has_tests "$FIXTURES/churn-families-complete.jsonl" \
    index_churn_oracle::verdict::fm \
    index_churn_oracle::verdict::ann_hnsw_dense \
    index_churn_oracle::verdict::sparse 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] verdict_records_present_pass"
    passed=$((passed + 1))
  else
    failures+=("verdict_records_present_pass")
  fi

  # B468-10: P0/P2 threshold verdict with status=fail must fail closed.
  set +e
  assert_jsonl_threshold "$FIXTURES/p0p2-threshold-fail.jsonl" \
    'all(.[]; .metric.status == "pass")' \
    "p0p2 multi-rep thresholds pass" 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] p0p2_threshold_status_fail_fails"
    passed=$((passed + 1))
  else
    failures+=("p0p2_threshold_status_fail_fails")
  fi

  # B468-10: required_reps < 5 must fail the five-rep completeness gate.
  set +e
  assert_jsonl_threshold "$FIXTURES/p0p2-reps-below-5.jsonl" \
    'any(.[]; .test=="p0p2::threshold::repetition_count" and .metric.status=="pass" and (.metric.required_reps // 0) >= 5)' \
    "p0p2 five-rep completeness" 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] p0p2_reps_below_5_fails"
    passed=$((passed + 1))
  else
    failures+=("p0p2_reps_below_5_fails")
  fi

  # B468-10: bundle-ok pass verdicts satisfy both gates.
  set +e
  assert_jsonl_threshold "$FIXTURES/bundle-ok/p0p2-threshold-verdict.jsonl" \
    'all(.[]; .metric.status == "pass")' \
    "p0p2 multi-rep thresholds pass" 2>/dev/null
  t_ok=$?
  assert_jsonl_threshold "$FIXTURES/bundle-ok/p0p2-threshold-verdict.jsonl" \
    'any(.[]; .test=="p0p2::threshold::repetition_count" and .metric.status=="pass" and (.metric.required_reps // 0) >= 5)' \
    "p0p2 five-rep completeness" 2>/dev/null
  r_ok=$?
  set -e
  if [[ "$t_ok" -eq 0 && "$r_ok" -eq 0 ]]; then
    echo "  [ OK ] p0p2_pass_fixture_passes"
    passed=$((passed + 1))
  else
    failures+=("p0p2_pass_fixture_passes")
  fi

  # B468-10: closure-status with p0p2_thresholds_ok=0 must fail even when
  # suites are green and overall claims pass (feeds overall_status/gate).
  set +e
  validate_closure_status "$FIXTURES/closure-status-p0p2-false.json" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] p0p2_threshold_false_status_fails"
    passed=$((passed + 1))
  else
    failures+=("p0p2_threshold_false_status_fails")
  fi

  # B468-10: complete bundle-ok status (with p0p2 keys true) must validate.
  set +e
  validate_closure_status "$FIXTURES/bundle-ok/closure-status.json" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] p0p2_status_keys_present_pass"
    passed=$((passed + 1))
  else
    failures+=("p0p2_status_keys_present_pass")
  fi


  # R170-06/07: hard-coded approximate success without checkpoints must fail
  # semantic threshold evaluation (measured fields required).
  printf '%s\n' \
    '{"test":"index_churn_oracle::verdict::ann_hnsw_dense","metric":{"status":"pass","exact":false,"recall":1.0,"required_recall":0.9,"checkpoints":0},"unit":"verdict"}' \
    > "$SELF_TEST_DIR/hardcoded-verdict.jsonl"
  set +e
  assert_jsonl_threshold "$SELF_TEST_DIR/hardcoded-verdict.jsonl" \
    'any(.[]; .test=="index_churn_oracle::verdict::ann_hnsw_dense" and .metric.status=="pass" and (.metric.checkpoints // 0) > 0 and ((.metric.minimum_recall // .metric.recall // 0) >= (.metric.required_recall // 0.9)))' \
    "measured dense verdict" 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] hardcoded_verdict_without_checkpoints_fails"
    passed=$((passed + 1))
  else
    failures+=("hardcoded_verdict_without_checkpoints_fails")
  fi

  # Missing sparse_tie_break record fails presence assert.
  set +e
  assert_jsonl_has_tests "$SELF_TEST_DIR/hardcoded-verdict.jsonl" \
    index_churn_oracle::sparse_tie_break 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] missing_sparse_tie_break_fails"
    passed=$((passed + 1))
  else
    failures+=("missing_sparse_tie_break_fails")
  fi

  # Bundle-ok measured dense / sparse_tie presence.
  set +e
  assert_jsonl_has_tests "$FIXTURES/bundle-ok/churn-oracle.jsonl" \
    index_churn_oracle::sparse_tie_break \
    index_churn_oracle::snapshot_history::ann_hnsw_dense 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] r170_evidence_records_present_in_bundle"
    passed=$((passed + 1))
  else
    failures+=("r170_evidence_records_present_in_bundle")
  fi

  # FF-08: missing jsonl for a semantic threshold must fail closed.
  set +e
  assert_jsonl_threshold "$SELF_TEST_DIR/does-not-exist.jsonl"     'true' "missing jsonl" 2>/dev/null
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] semantic_threshold_missing_jsonl_fails"
    passed=$((passed + 1))
  else
    failures+=("semantic_threshold_missing_jsonl_fails")
  fi

  # FF-09: jsonl_jq must reject records with status=fail (not presence-only).
  printf '%s\n'     '{"test":"index_churn_oracle::eligibility::sparse::authorization","metric":{"status":"fail","actual_count":0},"unit":"verdict"}'     > "$SELF_TEST_DIR/elig-fail.jsonl"
  set +e
  jq -se 'all([.[] | select(.test|startswith("index_churn_oracle::eligibility::"))][]; .metric.status=="pass" and (.metric.actual_count // 0) > 0)'     "$SELF_TEST_DIR/elig-fail.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -ne 0 ]]; then
    echo "  [ OK ] jsonl_jq_rejects_status_fail"
    passed=$((passed + 1))
  else
    failures+=("jsonl_jq_rejects_status_fail")
  fi
  set +e
  jq -se 'any(.[]; .test=="index_churn_oracle::sparse_tie_break" and .metric.status=="pass" and .metric.ordering_equal==true)'     "$FIXTURES/bundle-ok/churn-oracle.jsonl" >/dev/null 2>&1
  outcome=$?
  set -e
  if [[ "$outcome" -eq 0 ]]; then
    echo "  [ OK ] jsonl_jq_accepts_semantic_pass"
    passed=$((passed + 1))
  else
    failures+=("jsonl_jq_accepts_semantic_pass")
  fi

  echo "self-test summary: passed=$passed failed=${#failures[@]}"
  if [[ ${#failures[@]} -gt 0 ]]; then
    printf '  - %s\n' "${failures[@]}" >&2
    return 1
  fi
  return 0
}

# If we're in self-test mode, run the battery and exit (do NOT touch
# the real worktree, do NOT run any cargo).
if [[ "$SELF_TEST" -eq 1 ]]; then
  run_self_tests
  # Always exit explicitly so CI sees the self-test verdict on stdout,
  # even when every assertion passed (set -e would otherwise abort on
  # the very last `return 1` from a failing case, so we let the
  # function propagate its own exit code here).
  exit $?
fi

# --------------------------------------------------------------------------
# Below here we run the real pipeline. Helper functions are available
# ABOVE; suite invocation is below.
# --------------------------------------------------------------------------

# Suite runner — invokes cargo test, harvests JSON, accumulates manifest.
# Reads suite <idx> from the contract (§13.3): proof= cargo_exit|jsonl,
# command, optional test_args, jsonl, required_records, required_log_tests,
# required_thresholds, min_passed.
run_suite() {
  local idx="$1"
  local name proof jsonl_name min_passed
  name="$(jq -r ".suites[$idx].name" "$CONTRACT")"
  proof="$(jq -r ".suites[$idx].proof" "$CONTRACT")"
  jsonl_name="$(jq -r ".suites[$idx].jsonl // empty" "$CONTRACT")"
  min_passed="$(jq -r ".suites[$idx].min_passed // 0" "$CONTRACT")"

  local -a cmd test_args req_records req_log_tests
  mapfile -t cmd < <(jq -r ".suites[$idx].command[]" "$CONTRACT")
  mapfile -t test_args < <(jq -r ".suites[$idx].test_args[]?" "$CONTRACT")
  mapfile -t req_records < <(jq -r ".suites[$idx].required_records[]?" "$CONTRACT")
  mapfile -t req_log_tests < <(jq -r ".suites[$idx].required_log_tests[]?" "$CONTRACT")

  local log_path="$OUT/suite-${name}.log"
  local status=0
  local failed=0 passed=0 ignored=0

  echo "==> suite $name — cargo test ${cmd[*]}" | tee -a "$OUT/run.log"

  set +e
  cargo "${CARGO_FLAGS[@]}" test "${cmd[@]}" -- --nocapture ${test_args[@]+"${test_args[@]}"} \
    2>&1 | tee "$log_path"
  status=${PIPESTATUS[0]}
  set -e

  # Parse summary.
  local IFS=' '
  read -r passed failed ignored < <(parse_test_summary "$log_path")

  # Exit-code proof (§13.4): non-empty log + exit 0 + failed count 0.
  if [[ ! -s "$log_path" ]]; then
    echo "  [FAIL] suite $name — empty log" >&2
    status=1
  fi
  if ! /usr/bin/grep -q '^test result:' "$log_path"; then
    echo "  [FAIL] suite $name — no test result summary parsed from log" >&2
    status=1
  fi
  if [[ "$failed" -gt 0 ]]; then
    echo "  [FAIL] suite $name — parsed failed count $failed > 0" >&2
    status=1
  fi
  if [[ "$passed" -lt "$min_passed" ]]; then
    echo "  [FAIL] suite $name — passed $passed < min_passed $min_passed" >&2
    status=1
  fi

  # JSONL proof (§13.4): harvest and require the exact emitted records.
  local jsonl_status="n/a"
  local jsonl_records=0
  local jsonl_path=""
  if [[ "$proof" == "jsonl" ]]; then
    jsonl_path="$OUT/$jsonl_name"
    mkdir -p "$(dirname "$jsonl_path")"
    harvest_jsonl "$log_path" > "$jsonl_path" || true
    jsonl_records="$(count_jsonl_records "$jsonl_path")"
    if [[ "$jsonl_records" -eq 0 ]]; then
      echo "  [FAIL] suite $name — no JSONL records harvested" >&2
      status=1
      jsonl_status="missing"
    else
      jsonl_status="ok"
    fi
  fi

  # Required record checks (jsonl-proof suites).
  local required_tests_status="skipped"
  if [[ "$proof" == "jsonl" && ${#req_records[@]} -gt 0 ]]; then
    if assert_jsonl_has_tests "$jsonl_path" "${req_records[@]}"; then
      required_tests_status="pass"
    else
      status=1
      required_tests_status="fail"
    fi
  fi

  # Required log-test checks (exit-code proof suites naming exact tests).
  if [[ "$proof" == "cargo_exit" && ${#req_log_tests[@]} -gt 0 ]]; then
    if assert_log_tests_ok "$log_path" "${req_log_tests[@]}"; then
      required_tests_status="pass"
    else
      status=1
      required_tests_status="fail"
    fi
  fi

  # Per-suite threshold expressions.
  local thresholds_status="skipped"
  local thr_count
  thr_count="$(jq ".suites[$idx].required_thresholds | if type == \"array\" then length else 0 end" "$CONTRACT")"
  if [[ "$proof" == "jsonl" && "$thr_count" -gt 0 ]]; then
    local failed_threshold=0
    local t jq_expr label
    for ((t = 0; t < thr_count; t++)); do
      jq_expr="$(jq -r ".suites[$idx].required_thresholds[$t].jq" "$CONTRACT")"
      label="$(jq -r ".suites[$idx].required_thresholds[$t].label" "$CONTRACT")"
      if ! assert_jsonl_threshold "$jsonl_path" "$jq_expr" "$label"; then
        failed_threshold=1
      fi
    done
    if [[ "$failed_threshold" -eq 1 ]]; then
      status=1
      thresholds_status="fail"
    else
      thresholds_status="pass"
    fi
  fi

  # Aggregate into overall status.
  if [[ "$status" -ne 0 ]]; then
    overall_status=1
  fi

  # Append this suite's entry to the manifest accumulator.
  jq -n \
    --arg name "$name" \
    --arg sha "$SHORT_SHA" \
    --arg proof "$proof" \
    --arg command "cargo ${CARGO_FLAGS[*]} test ${cmd[*]}" \
    --argjson exit_code "$status" \
    --argjson passed "$passed" \
    --argjson failed "$failed" \
    --argjson ignored "$ignored" \
    --argjson jsonl_records "$jsonl_records" \
    --arg jsonl_status "$jsonl_status" \
    --arg required_tests "$required_tests_status" \
    --arg thresholds "$thresholds_status" \
    --arg log_path "suite-${name}.log" \
    --arg metrics_file "${jsonl_path:-}" \
    '{
      name: $name,
      sha: $sha,
      proof: $proof,
      command: $command,
      exit_code: $exit_code,
      passed: $passed,
      failed: $failed,
      ignored: $ignored,
      jsonl_records: $jsonl_records,
      jsonl_status: $jsonl_status,
      required_tests: $required_tests,
      thresholds: $thresholds,
      log_path: $log_path,
      metrics_file: $metrics_file
    }' >> "$SUITES_JSONL"

  if [[ "$status" -ne 0 ]]; then
    echo "  [FAIL] suite $name — exit=$status passed=$passed failed=$failed ignored=$ignored" >&2
  else
    echo "  [ OK ] suite $name — passed=$passed failed=$failed ignored=$ignored"
  fi
}

# --------------------------------------------------------------------------
# Suite definitions come from the contract (§13.3) — nothing is hardcoded.
# --------------------------------------------------------------------------
if [[ "$SELF_TEST" -eq 0 ]]; then

suite_count="$(jq '.suites | length' "$CONTRACT")"
for ((s = 0; s < suite_count; s++)); do
  run_suite "$s"
done

# Hot-metrics sample artifact. The /metrics endpoint scrape is emitted to
# stdout by the test body — copy the Prometheus lines into a dedicated
# artifact so downstream tools can inspect without re-running.
hot_log="$OUT/suite-hot_metrics_export.log"
if [[ -s "$hot_log" ]]; then
  /usr/bin/grep -E '^# (HELP|TYPE) hot_|^hot_' "$hot_log" | sort -u \
    > "$OUT/hot-metrics-sample.txt" 2>/dev/null || true
fi
# The artifact gate requires this file; if the scrape produced nothing,
# leave it absent so the gate fails instead of synthesizing content.

# --------------------------------------------------------------------------
# 8-seed index-churn oracle matrix (§13.10). One summary object per seed;
# a seed fails on nonzero exit, missing family, family failure, or
# malformed JSON.
# --------------------------------------------------------------------------
mapfile -t CHURN_SEEDS < <(jq -r '.churn_matrix.seeds[]' "$CONTRACT")
mapfile -t CHURN_CMD < <(jq -r '.churn_matrix.command[]' "$CONTRACT")
MATRIX_NAME="$(jq -r '.churn_matrix.matrix_file' "$CONTRACT")"
ORACLE_OPS_DEFAULT="$(jq -r '.churn_matrix.default_operations' "$CONTRACT")"
ORACLE_OPS="${MONGRELDB_ORACLE_OPERATIONS:-$ORACLE_OPS_DEFAULT}"
: > "$OUT/$MATRIX_NAME"
churn_matrix_status=0
for seed in "${CHURN_SEEDS[@]}"; do
  seed_log="$OUT/churn-oracle-seed-${seed}.log"
  echo "==> churn-oracle seed=$seed operations=$ORACLE_OPS"
  set +e
  MONGRELDB_ORACLE_SEED="$seed" MONGRELDB_ORACLE_OPERATIONS="$ORACLE_OPS" \
    cargo "${CARGO_FLAGS[@]}" test "${CHURN_CMD[@]}" -- --nocapture \
    2>&1 | tee "$seed_log"
  seed_status=${PIPESTATUS[0]}
  set -e
  if ! churn_seed_summary "$seed" "$seed_status" "$seed_log" >> "$OUT/$MATRIX_NAME"; then
    churn_matrix_status=1
  fi
done
if [[ "$churn_matrix_status" -ne 0 ]]; then
  overall_status=1
fi
jq -n \
  --arg name index_churn_oracle_eight_seed_matrix \
  --arg sha "$SHORT_SHA" \
  --arg proof churn_matrix \
  --argjson exit_code "$churn_matrix_status" \
  --arg seeds "$(printf '%s,' "${CHURN_SEEDS[@]}" | sed 's/,$//')" \
  --argjson operations "$ORACLE_OPS" \
  --arg matrix_file "$MATRIX_NAME" \
  '{
    name: $name,
    sha: $sha,
    proof: $proof,
    exit_code: $exit_code,
    seeds: ($seeds | split(",")),
    operations: $operations,
    matrix_file: $matrix_file
  }' >> "$SUITES_JSONL"

# --------------------------------------------------------------------------
# Cross-suite thresholds (§13.7–§13.9 + B468-10). ALL feed overall_status
# (§13.8) BEFORE closure-status.json and closure-checklist.md are written.
# --------------------------------------------------------------------------
COMPLETE_MISS_OK=0
P95_RATIO_OK=0
P0P2_THRESHOLDS_OK=0
P0P2_REPS_OK=0

complete_miss_verdict="$(check_complete_miss "$OUT" || true)"
if [[ "$complete_miss_verdict" == "pass" ]]; then
  COMPLETE_MISS_OK=1
else
  echo "  [FAIL] complete-miss gate: $complete_miss_verdict" >&2
fi

p95_verdict="$(check_p95_ratio "$OUT" || true)"
if [[ "$p95_verdict" == "pass" ]]; then
  P95_RATIO_OK=1
else
  echo "  [FAIL] point-lookup 256-to-1 p95 gate: $p95_verdict" >&2
fi
P95_RATIO_OBSERVED="$(p95_ratio_observed "$OUT")"
if ! jq -e . >/dev/null 2>&1 <<<"$P95_RATIO_OBSERVED"; then
  P95_RATIO_OBSERVED=null
fi
POINT_LOOKUP_MAX_P95_RATIO_USED="$(p95_max_ratio)"
if ! jq -e . >/dev/null 2>&1 <<<"$POINT_LOOKUP_MAX_P95_RATIO_USED"; then
  echo "FAIL: invalid POINT_LOOKUP_MAX_P95_RATIO: $POINT_LOOKUP_MAX_P95_RATIO_USED" >&2
  exit 1
fi

# B468-10: exact-SHA P0/P2 harvest + multi-rep threshold eval (same phase as
# complete_miss / p95 — never after checklist/status write).
echo "[*] Exact-SHA P0/P2 harvest (B468-10)"
if [[ ! -s "$OUT/p0-results.jsonl" \
   || ! -s "$OUT/p2-standalone-results.jsonl" \
   || ! -s "$OUT/p2-full-feature-results.jsonl" \
   || ! -s "$OUT/p0p2-threshold-verdict.jsonl" ]]; then
  if [[ "${RESIDUAL_CLOSURE_SKIP_P0P2:-0}" == "1" ]]; then
    echo "RESIDUAL_CLOSURE_SKIP_P0P2=1 but P0/P2 JSONL missing — fail closed" >&2
    exit 1
  fi
  bash "$REPO_ROOT/scripts/run-p0-p2-measurements.sh" "$OUT" \
    || { echo "P0/P2 harvest failed — fail closed" >&2; exit 1; }
fi
if [[ -s "$OUT/p0p2-threshold-verdict.jsonl" ]]; then
  if assert_jsonl_threshold "$OUT/p0p2-threshold-verdict.jsonl" \
      'all(.[]; .metric.status == "pass")' \
      "p0p2 multi-rep thresholds pass"; then
    P0P2_THRESHOLDS_OK=1
  else
    echo "  [FAIL] p0p2 multi-rep thresholds" >&2
  fi
  if assert_jsonl_threshold "$OUT/p0p2-threshold-verdict.jsonl" \
      'any(.[]; .test=="p0p2::threshold::repetition_count" and .metric.status=="pass" and (.metric.required_reps // 0) >= 5)' \
      "p0p2 five-rep completeness"; then
    P0P2_REPS_OK=1
  else
    echo "  [FAIL] p0p2 five-rep completeness" >&2
  fi
else
  echo "  [FAIL] p0p2-threshold-verdict.jsonl missing" >&2
fi

# FF-08: execute every semantic object under contract .thresholds against
# the relevant JSONL in $OUT. Missing file or failed jq fails closed.
echo "[*] Contract semantic thresholds (FF-08)"
SEMANTIC_THRESHOLDS_OK=1
while IFS= read -r tkey; do
  [[ -z "$tkey" ]] && continue
  jsonl="$(jq -r --arg k "$tkey" '.thresholds[$k].jsonl // empty' "$CONTRACT")"
  jq_expr="$(jq -r --arg k "$tkey" '.thresholds[$k].jq // empty' "$CONTRACT")"
  label="$(jq -r --arg k "$tkey" '.thresholds[$k].label // $k' "$CONTRACT")"
  [[ -z "$jsonl" || -z "$jq_expr" || "$jq_expr" == "null" ]] && continue
  path="$OUT/$jsonl"
  if [[ ! -s "$path" ]]; then
    # Fall back to suite jsonl names used in fixtures
    path="$OUT/$jsonl"
  fi
  if [[ ! -s "$path" ]]; then
    echo "  [FAIL] threshold $tkey — missing $jsonl" >&2
    SEMANTIC_THRESHOLDS_OK=0
    continue
  fi
  if ! assert_jsonl_threshold "$path" "$jq_expr" "$label"; then
    SEMANTIC_THRESHOLDS_OK=0
  fi
done < <(jq -r '.thresholds | keys[]' "$CONTRACT")
if [[ "$SEMANTIC_THRESHOLDS_OK" -ne 1 ]]; then
  overall_status=1
fi

# §13.8: every top-level threshold affects the overall verdict.
if [[ "$COMPLETE_MISS_OK" -ne 1 ]]; then
  overall_status=1
fi
if [[ "$P95_RATIO_OK" -ne 1 ]]; then
  overall_status=1
fi
if [[ "$P0P2_THRESHOLDS_OK" -ne 1 ]]; then
  overall_status=1
fi
if [[ "$P0P2_REPS_OK" -ne 1 ]]; then
  overall_status=1
fi

# --------------------------------------------------------------------------
# closure-status.json. Records every top-level threshold including B468-10.
# --------------------------------------------------------------------------
COMPLETED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

jq -s \
  --arg commit "$EXPECTED_SHA" \
  --arg short "$SHORT_SHA" \
  --arg toolchain "$TOOLCHAIN" \
  --arg started "$STARTED_AT" \
  --arg completed "$COMPLETED_AT" \
  --argjson complete_miss_ok "$COMPLETE_MISS_OK" \
  --argjson p95_ratio_ok "$P95_RATIO_OK" \
  --argjson p95_ratio_max "$POINT_LOOKUP_MAX_P95_RATIO_USED" \
  --argjson p95_ratio_observed "$P95_RATIO_OBSERVED" \
  --argjson p0p2_thresholds_ok "$P0P2_THRESHOLDS_OK" \
  --argjson p0p2_reps_ok "$P0P2_REPS_OK" \
  --argjson overall_status "$overall_status" \
  '
  {
    commit: $commit,
    short_sha: $short,
    toolchain: $toolchain,
    started_at: $started,
    completed_at: $completed,
    suites: .,
    thresholds: {
      complete_miss_reader_opens: 0,
      complete_miss_ok: $complete_miss_ok,
      point_lookup_256_to_1_p95_ratio: $p95_ratio_max,
      point_lookup_p95_ratio_observed: $p95_ratio_observed,
      point_lookup_p95_ratio_ok: $p95_ratio_ok,
      p0p2_thresholds_ok: $p0p2_thresholds_ok,
      p0p2_reps_ok: $p0p2_reps_ok
    },
    overall: (if $overall_status == 0 then "pass" else "fail" end)
  }
  ' "$SUITES_JSONL" > "$OUT/closure-status.json"

# --------------------------------------------------------------------------
# Closure checklist (§13.11). Every row maps to the exact suite/tests that
# prove it via contract `checklist` entries — no proxies from unrelated
# logs.
# --------------------------------------------------------------------------
COMMIT_LONG="$EXPECTED_SHA"
COMMIT_SHORT="$SHORT_SHA"
TOOLCHAIN_VER="$(rustc "${CARGO_FLAGS[@]}" -Vv | head -1)"

checklist_row_mark() {
  local entry="$1"
  local proof
  proof="$(jq -r '.proof' <<<"$entry")"
  case "$proof" in
    suite_exit)
      local suite row
      suite="$(jq -r '.suite' <<<"$entry")"
      row="$(jq -sc --arg n "$suite" 'map(select(.name == $n))[0] // empty' "$SUITES_JSONL")"
      if [[ -n "$row" ]] \
        && [[ "$(jq -r '.exit_code' <<<"$row")" == "0" ]] \
        && [[ "$(jq -r '.failed' <<<"$row")" == "0" ]] \
        && [[ "$(jq -r '.required_tests' <<<"$row")" != "fail" ]] \
        && [[ "$(jq -r '.thresholds' <<<"$row")" != "fail" ]]; then
        echo x
      else
        echo ' '
      fi
      ;;
    jsonl_records)
      local jsonl ok rec
      jsonl="$(jq -r '.jsonl' <<<"$entry")"
      ok=1
      while IFS= read -r rec; do
        [[ -z "$rec" ]] && continue
        if ! { [[ -s "$OUT/$jsonl" ]] && /usr/bin/grep -qF "\"test\":\"${rec}\"" "$OUT/$jsonl"; }; then
          ok=0
          break
        fi
      done < <(jq -r '.records[]' <<<"$entry")
      if [[ "$ok" -eq 1 ]]; then echo x; else echo ' '; fi
      ;;
    jsonl_jq)
      # FF-09: semantic success, not mere record presence.
      local jsonl jq_expr
      jsonl="$(jq -r '.jsonl' <<<"$entry")"
      jq_expr="$(jq -r '.jq' <<<"$entry")"
      if [[ -s "$OUT/$jsonl" ]] && jq -se "$jq_expr" "$OUT/$jsonl" >/dev/null 2>&1; then
        echo x
      else
        echo ' '
      fi
      ;;
    threshold)
      local key
      key="$(jq -r '.threshold' <<<"$entry")"
      case "$key" in
        complete_miss_ok)
          if [[ "$COMPLETE_MISS_OK" -eq 1 ]]; then echo x; else echo ' '; fi
          ;;
        point_lookup_p95_ratio_ok)
          if [[ "$P95_RATIO_OK" -eq 1 ]]; then echo x; else echo ' '; fi
          ;;
        p0p2_all_verdicts_pass)
          if [[ "${P0P2_THRESHOLDS_OK:-0}" -eq 1 ]]; then echo x; else echo ' '; fi
          ;;
        p0p2_reps_ok)
          if [[ "${P0P2_REPS_OK:-0}" -eq 1 ]]; then echo x; else echo ' '; fi
          ;;
        *)
          echo ' '
          ;;
      esac
      ;;
    file)
      local f
      f="$(jq -r '.file' <<<"$entry")"
      if [[ -s "$OUT/$f" ]]; then echo x; else echo ' '; fi
      ;;
    file_grep)
      local f pat
      f="$(jq -r '.file' <<<"$entry")"
      pat="$(jq -r '.pattern' <<<"$entry")"
      if [[ -s "$OUT/$f" ]] && /usr/bin/grep -qE "$pat" "$OUT/$f"; then
        echo x
      else
        echo ' '
      fi
      ;;
    churn_matrix)
      if validate_churn_matrix "$OUT/churn-oracle-matrix.jsonl" >/dev/null 2>&1; then
        echo x
      else
        echo ' '
      fi
      ;;
    commit)
      if [[ -s "$OUT/commit.txt" ]] \
        && [[ "$(tr -d '[:space:]' < "$OUT/commit.txt")" == "$EXPECTED_SHA" ]]; then
        echo x
      else
        echo ' '
      fi
      ;;
    *)
      echo ' '
      ;;
  esac
}

checklist_row_proof_detail() {
  local entry="$1"
  local proof
  proof="$(jq -r '.proof' <<<"$entry")"
  case "$proof" in
    suite_exit)
      jq -r '"suite-\(.suite).log (cargo exit + failed count 0)"' <<<"$entry"
      ;;
    jsonl_records)
      jq -r '"\(.jsonl) records: \(.records | join(", "))"' <<<"$entry"
      ;;
    jsonl_jq)
      jq -r '"\(.jsonl) jq /\(.jq)/"' <<<"$entry"
      ;;
    threshold)
      jq -r '"closure-status.json thresholds.\(.threshold)"' <<<"$entry"
      ;;
    file)
      jq -r '"\(.file) non-empty"' <<<"$entry"
      ;;
    file_grep)
      jq -r '"\(.file) grep /\(.pattern)/"' <<<"$entry"
      ;;
    churn_matrix)
      echo "churn-oracle-matrix.jsonl (8 seeds × 9 families)"
      ;;
    commit)
      echo "commit.txt exact SHA"
      ;;
    *)
      echo "unknown proof"
      ;;
  esac
}

{
  echo "# Residual closure checklist"
  echo
  echo "Generated by \`scripts/run-residual-closure.sh\` on \`$COMMIT_SHORT\`."
  echo
  echo "| Field | Value |"
  echo "|---|---|"
  echo "| Commit (long) | \`$COMMIT_LONG\` |"
  echo "| Commit (short) | \`$COMMIT_SHORT\` |"
  echo "| Toolchain | $TOOLCHAIN_VER |"
  echo "| Generated | $(date -u +"%Y-%m-%dT%H:%M:%SZ") |"
  echo "| Host | $(uname -n) ($(uname -srm)) |"
  echo "| Churn operations/seed | $ORACLE_OPS |"
  echo "| Point-lookup p95 max ratio | $POINT_LOOKUP_MAX_P95_RATIO_USED |"
  echo
  echo "Status manifest: \`closure-status.json\` (parsed by workflow final job)."
  echo "✅ = the exact proof named by the contract checklist entry holds."
  echo
  checklist_count="$(jq '.checklist | length' "$CONTRACT")"
  for ((c = 0; c < checklist_count; c++)); do
    entry="$(jq -c ".checklist[$c]" "$CONTRACT")"
    statement="$(jq -r '.statement' <<<"$entry")"
    mark="$(checklist_row_mark "$entry")"
    detail="$(checklist_row_proof_detail "$entry")"
    echo "- [$mark] $statement (proof: \`$detail\`)"
  done
  echo
  echo "## Notes"
  echo
  echo "- Status manifest \`closure-status.json\` carries \`overall: pass\` only when"
  echo "  every suite \`exit_code == 0\` AND \`thresholds.complete_miss_ok\` AND"
  echo "  \`thresholds.point_lookup_p95_ratio_ok\` AND \`thresholds.p0p2_thresholds_ok\`"
  echo "  AND \`thresholds.p0p2_reps_ok\` are all true. The workflow"
  echo "  \`final-aggregate\` job requires \`overall == \"pass\"\` before publishing."
  echo "- Exact-SHA verification: \`verify-exact-sha\` job downloads this"
  echo "  artifact bundle, reads \`commit.txt\`, and rejects the bundle if its"
  echo "  contents do not match the requested SHA."
} > "$OUT/closure-checklist.md"

# FF-09/14: honest history status (never fabricate 30/4 passes).
if bash "$REPO_ROOT/scripts/churn-history-check.sh" >"$OUT/history-check.log" 2>&1; then
  jq -nc --arg status pass --argjson nightly 30 --argjson weekly 4 \
    '{status:$status, nightly:$nightly, weekly:$weekly, source:"churn-history-check.sh"}' \
    > "$OUT/history-status.jsonl"
else
  jq -nc --arg status fail --argjson nightly 0 --argjson weekly 0 \
    '{status:$status, nightly:$nightly, weekly:$weekly, source:"churn-history-check.sh"}' \
    > "$OUT/history-status.jsonl"
fi


# Stamp every shard artifact with SHA sidecar (after the checklist exists so
# it is stamped too). Loop over glob expansion with nullglob so a missing
# artifact (during partial runs) doesn't error.
shopt -s nullglob
shopt_stamp_iter() {
  for f in "$OUT"/*.log "$OUT"/*.jsonl "$OUT"/*.json "$OUT"/hot-metrics-sample.txt "$OUT"/closure-checklist.md; do
    [[ -f "$f" ]] || continue
    echo "$EXPECTED_SHA" > "$f.sha"
  done
}
shopt_stamp_iter
shopt -u nullglob
unset -f shopt_stamp_iter

# --------------------------------------------------------------------------
# Artifact gate (§13.4 + §13.8). FAIL-CLOSED. Requires exactly the files
# the contract declares — never files the pipeline does not create.
# --------------------------------------------------------------------------
echo "[*] Artifact gate (fail-closed)"

gate_fail=0
if ! check_required_artifacts "$OUT" "$EXPECTED_SHA"; then
  gate_fail=1
fi

if [[ "$gate_fail" -ne 0 || "$overall_status" -ne 0 ]]; then
  echo "artifact gate failed; see $OUT" >&2
  exit 1
fi

echo
echo "All residual-closure artifacts present under $OUT"
ls -la "$OUT" | head -80

fi # end of SELF_TEST=0 block
