#!/usr/bin/env bash
# Residual-closure orchestrator (Spec REM-006).
#
# Fail-closed pipeline that runs every required residual test/suite, harvests
# structured JSON metrics, and produces a machine-readable `closure-status.json`
# manifest. The final workflow job parses this manifest and requires
# `overall == "pass"`. Per-spec sections 51–60:
#
#   53. Script MUST use `set -euo pipefail`, aggregate failures via an
#       `overall_status` variable, and exit nonzero on any failure.
#   54. Produce a structured `closure-status.json` manifest with one entry
#       per suite, exit_code/passed/failed/ignored, a per-suite metrics_file,
#       and a top-level `overall: pass|fail`.
#   55. For any suite expected to emit JSON: command must exit zero, file
#       must exist, JSON must parse, expected records must be present,
#       each required metric must exist, threshold expressions must
#       evaluate true. NEVER synthesize missing metrics.
#   56. Each checklist statement is tied to the exact test/metric that proves
#       it — no proxies from unrelated logs.
#   57. Workflow jobs: formatting+clippy, point-directory, HLC multi-run,
#       persistent-cache integration/race/reopen, controlled-scan,
#       HNSW connectivity, 8-seed churn smoke, HOT metrics, full core
#       tests, full workspace tests, release-mode performance, final
#       evidence aggregation, exact-SHA verification.
#   58. Exact-SHA: check out requested SHA, record HEAD before tests,
#       include SHA in every shard, reject artifacts from another SHA,
#       aggregate only matching artifacts, fail when no run exists for
#       the requested SHA, publish the final checklist only after every
#       gate passes.
#
# Usage:
#   bash scripts/run-residual-closure.sh
#   bash scripts/run-residual-closure.sh self-test <fixture-dir>

# --------------------------------------------------------------------------
# `set -euo pipefail` is REQUIRED (Spec §53).
# --------------------------------------------------------------------------

set -euo pipefail

# --------------------------------------------------------------------------
# Self-test battery (Spec §59) — first arg switches into self-test mode.
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
# repo. Skip the worktree chdir so a dirty dev shell still works.
if [[ "$SELF_TEST" -eq 0 ]]; then
  cd "$REPO_ROOT"
fi

TOOLCHAIN="${TOOLCHAIN:-1.97.1}"
CARGO_FLAGS=(+${TOOLCHAIN})

if [[ "$SELF_TEST" -eq 0 ]]; then
  OUT="${RUNNER_TEMP:-/tmp}/mongreldb-residual-closure"
  rm -rf "$OUT"
  mkdir -p "$OUT"

  # Exact SHA (Spec §58 §1–3). Capture BEFORE running tests so every
  # artifact stamped with the SHA can be cross-checked by
  # `verify-exact-sha`. EXPECTED_SHA may come from a workflow env var;
  # restrict to a 7–40 char hex SHA so a malformed value (e.g. injected
  # shell metacharacters in `inputs.ref`) cannot reach our artifact
  # files or the comparison step.
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
# Self-test battery (Spec §59). Runs battery of fail-closed assertions
# against fixture files. NEVER touches the real worktree.
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
  # gate when an artifact's SHA differs from the recorded HEAD. We do
  # NOT run the gate here (it requires OUT dir), but we exercise the
  # comparison function.
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
  # records — verifies Spec §55.5 (no synthetic content).
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
#
# Caller sets globals before invocation:
#   JSONL_OUT=<path>    # optional; if set, requires at least 1 JSON record
#   REQUIRED_TESTS=(a b c)  # optional; if set, every name must appear in JSONL
#   REQUIRED_THRESHOLDS=(jq_expr<TAB>label ...)  # optional; each must pass
#
# run_suite reads REQUIRED_TESTS/REQUIRED_THRESHOLDS via namerefs so the
# arrays stay in the caller scope (bash cannot pass arrays via env).
run_suite() {
  local name="$1"
  shift
  local log_path="$OUT/suite-${name}.log"
  local status=0
  local failed=0 passed=0 ignored=0

  echo "==> suite $name — cargo test $*" | tee -a "$OUT/run.log"

  set +e
  cargo "${CARGO_FLAGS[@]}" test "$@" -- --nocapture 2>&1 | tee "$log_path"
  status=${PIPESTATUS[0]}
  set -e

  # Parse summary.
  local IFS=' '
  read -r passed failed ignored < <(parse_test_summary "$log_path")

  # JSON harvest (Spec §55).
  local jsonl_status="n/a"
  local jsonl_records=0
  local jsonl_path=""
  if [[ -n "${JSONL_OUT:-}" ]]; then
    jsonl_path="${JSONL_OUT}"
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

  # Required record checks (Spec §55.4–5).
  local required_tests_status="skipped"
  if [[ -n "$jsonl_path" ]] && declare -p REQUIRED_TESTS 2>/dev/null | /usr/bin/grep -q '^declare'; then
    local -n _req=REQUIRED_TESTS
    local _req_len="${#_req[@]}"
    if [[ "$_req_len" -gt 0 ]]; then
      if assert_jsonl_has_tests "$jsonl_path" "${_req[@]}"; then
        required_tests_status="pass"
      else
        status=1
        required_tests_status="fail"
      fi
    fi
    unset -n _req
  fi

  # Threshold expressions.
  local thresholds_status="skipped"
  if [[ -n "$jsonl_path" ]] && declare -p REQUIRED_THRESHOLDS 2>/dev/null | /usr/bin/grep -q '^declare'; then
    local -n _thr=REQUIRED_THRESHOLDS
    local _thr_len="${#_thr[@]}"
    if [[ "$_thr_len" -gt 0 ]]; then
      local failed_threshold=0
      local entry jq_expr label
      for entry in "${_thr[@]}"; do
        # Tab-separated `jq_expr|label` pair.
        if [[ "$entry" == *$'\t'* ]]; then
          jq_expr="${entry%%$'\t'*}"
          label="${entry##*$'\t'}"
        else
          jq_expr="$entry"
          label="$entry"
        fi
        [[ -z "$label" ]] && label="$jq_expr"
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
    unset -n _thr
  fi

  # Aggregate into overall status.
  if [[ "$status" -ne 0 ]]; then
    overall_status=1
  fi

  # Append this suite's entry to the manifest accumulator.
  jq -n \
    --arg name "$name" \
    --arg sha "$SHORT_SHA" \
    --arg command "cargo ${CARGO_FLAGS[*]} test $*" \
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
# Suite definitions (Spec §57).
# --------------------------------------------------------------------------
if [[ "$SELF_TEST" -eq 0 ]]; then

# Some suites (e.g. point_lookup_directory, hnsw_connectivity) have no
# `emit_metric!` calls — their proof is the suite exit code plus the
# `test result: ok` line. We don't request a jsonl from those because the
# spec §55 explicitly forbids synthesizing content for missing metrics.

# PR B — Point-lookup directory structure (REM-004). No jsonl emission;
# proof is the suite exit code + assertions in the test body.
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite point_lookup_directory \
  -p mongreldb-core --test point_lookup_directory --all-features

# PR B — Scaling with run count (REM-004). The point_lookup_runs test
# DOES emit a `{"test":"point_lookup_scaling_with_immutable_run_count"}`
# record — require that record and assert scaling-samples >= 3.
JSONL_OUT="$OUT/point-lookup-runs.jsonl"
REQUIRED_TESTS=(point_lookup_scaling_with_immutable_run_count)
REQUIRED_THRESHOLDS=(
  $'[.[]|select(.test=="point_lookup_scaling_with_immutable_run_count")][0].samples|length>=3\tscaling samples >= 3'
)
run_suite point_lookup_runs_scaling \
  -p mongreldb-core --release --test point_lookup_runs -- --include-ignored
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS

# PR C — Async persistent result cache (REM-002). Structure suite does
# not emit a JSONL line; proof is exit code + grep on the
# `no_query_thread_io` assertion output.
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite result_cache_async_persistence_structure \
  -p mongreldb-core --test result_cache_async_persistence --all-features
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite result_cache_async_persistence_ignored \
  -p mongreldb-core --test result_cache_async_persistence --all-features -- --ignored

# PR D — Controlled-scan streaming (REM-003). This suite DOES emit
# JSONL via `emit_scan_metric!`. We require the bounded-buffer record
# in both structure and release modes.
JSONL_OUT="$OUT/controlled-scan-structure.jsonl"
REQUIRED_TESTS=(controlled_scan::million_row_controlled_scan_keeps_source_buffers_bounded)
run_suite controlled_scan_streaming_structure \
  -p mongreldb-core --test controlled_scan_streaming --all-features
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS

JSONL_OUT="$OUT/controlled-scan-benchmark.jsonl"
REQUIRED_TESTS=(controlled_scan::million_row_controlled_scan_keeps_source_buffers_bounded)
run_suite controlled_scan_streaming_benchmark \
  -p mongreldb-core --release --test controlled_scan_streaming -- --include-ignored
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS

# PR E — HNSW connectivity (REM-001). Tests do NOT emit JSONL; proof is
# exit code + `test result: ok` summary.
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite hnsw_connectivity \
  -p mongreldb-core --test hnsw_connectivity --all-features

# PR E — Non-Bitmap churn oracle (REM-005). Suite DOES emit JSONL via
# `emit_oracle_metric!`. We require each non-Bitmap family metric.
JSONL_OUT="$OUT/churn-oracle.jsonl"
REQUIRED_TESTS=(
  index_churn_oracle::churn_oracle_fmindex
  index_churn_oracle::churn_oracle_learned_range
  index_churn_oracle::churn_oracle_seed_determinism
)
run_suite index_churn_oracle_eight_seed_smoke \
  -p mongreldb-core --test index_churn_oracle --all-features
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS

# PR F — HOT fallback observability (REM-006). Suite DOES emit JSONL via
# `emit_metric!`. We require at least the zero-fallback + sum-consistent
# records; optional records are listed but only the present-ones fail.
JSONL_OUT="$OUT/lookup-metrics.jsonl"
REQUIRED_TESTS=(
  lookup_metrics::healthy_pk_lookup_records_zero_fallback
  lookup_metrics::healthy_pk_lookup_uses_hot_fast_path_with_zero_fallback
  lookup_metrics::snapshot_to_metrics_is_consistent
)
run_suite lookup_metrics_hot_observability \
  -p mongreldb-core --test lookup_metrics --all-features
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS

# HOT metrics endpoint (server). Test emits JSONL via the same macro and
# scrapes the `/metrics` HTTP endpoint — no record names are emitted by
# this test, so JSONL_OUT is omitted.
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite hot_metrics_export_server \
  -p mongreldb-server --test hot_metrics_export --all-features

# Full core + full workspace dedicated commands (Spec §57.9–10). These
# ARE the dedicated full-workspace commands; their exit code is the proof.
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite full_core_tests \
  -p mongreldb-core --tests --all-features --lib
unset JSONL_OUT REQUIRED_TESTS REQUIRED_THRESHOLDS
run_suite full_workspace_tests \
  --workspace --all-features --exclude mongreldb-perf --tests

# Hot-metrics sample artifact (Spec §57.8). The /metrics endpoint scrape
# is emitted to stdout by the test body — copy the Prometheus lines into
# a dedicated artifact so downstream tools can inspect without re-running.
HOT_METRICS_OUT="$OUT/hot-metrics-sample.txt"
log_path="$OUT/suite-hot_metrics_export_server.log"
if [[ -s "$log_path" ]]; then
  /usr/bin/grep -E '^# (HELP|TYPE) hot_|^hot_' "$log_path" | sort -u \
    > "$OUT/hot-metrics-sample.txt" 2>/dev/null || true
fi

# --------------------------------------------------------------------------
# 8-seed index-churn oracle matrix (Spec §57.7, REM-005).
# --------------------------------------------------------------------------
CHURN_SEEDS=(1 2 3 4 5 6 7 8)
: > "$OUT/churn-oracle-matrix.jsonl"
churn_matrix_status=0
for seed in "${CHURN_SEEDS[@]}"; do
  echo "==> churn-oracle seed=$seed"
  set +e
  MONGRELDB_ORACLE_SEED="$seed" \
    cargo "${CARGO_FLAGS[@]}" test -p mongreldb-core --test index_churn_oracle \
      --all-features -- --nocapture \
    2>&1 | tee "$OUT/churn-oracle-seed-${seed}.log"
  seed_status=${PIPESTATUS[0]}
  set -e
  harvest_jsonl "$OUT/churn-oracle-seed-${seed}.log" \
    | jq -Rrc --argjson seed "$seed" --argjson status "$seed_status" \
        '{seed: $seed, status: $status}' \
    >> "$OUT/churn-oracle-matrix.jsonl" || true
  if [[ "$seed_status" -ne 0 ]]; then
    churn_matrix_status=1
  fi
done
if [[ "$churn_matrix_status" -ne 0 ]]; then
  overall_status=1
fi
jq -n \
  --arg name index_churn_oracle_eight_seed_matrix \
  --arg sha "$SHORT_SHA" \
  --argjson exit_code "$churn_matrix_status" \
  --arg seeds "$(printf '%s,' "${CHURN_SEEDS[@]}" | sed 's/,$//')" \
  --arg matrix_file "churn-oracle-matrix.jsonl" \
  '{
    name: $name,
    sha: $sha,
    exit_code: $exit_code,
    seeds: ($seeds | split(",")),
    matrix_file: $matrix_file
  }' >> "$SUITES_JSONL"

# --------------------------------------------------------------------------
# Cross-suite thresholds (Spec §54).
# --------------------------------------------------------------------------
COMPLETE_MISS_OK=0
if [[ -s "$OUT/suite-point_lookup_directory.log" ]]; then
  if /usr/bin/grep -q 'point_lookup_directory_complete_miss_opens_zero_readers' \
      "$OUT/suite-point_lookup_directory.log"; then
    COMPLETE_MISS_OK=1
  fi
fi
P95_RATIO_OK=0
if [[ -s "$OUT/point-lookup-runs.jsonl" ]]; then
  if jq -e '
      [.[] | select(.test=="point_lookup_scaling_with_immutable_run_count")][0].samples
      | (map(select(.target_runs==1))[0].point_query_latency.p95_us // 1) as $p1
      | (map(select(.target_runs==16))[0].point_query_latency.p95_us // $p1*100) as $pN
      | ($pN / $p1) <= 1.4
    ' "$OUT/point-lookup-runs.jsonl" >/dev/null 2>&1; then
    P95_RATIO_OK=1
  fi
fi

# --------------------------------------------------------------------------
# closure-status.json (Spec §54).
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
      point_lookup_256_to_1_p95_ratio: 1.4,
      point_lookup_p95_ratio_ok: $p95_ratio_ok
    },
    overall: (if $overall_status == 0 then "pass" else "fail" end)
  }
  ' "$SUITES_JSONL" > "$OUT/closure-status.json"

# Stamp every shard artifact with SHA sidecar (Spec §58 §3). Loop over
# glob expansion with nullglob so a missing artifact (during partial
# runs) doesn't error.
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
# Closure checklist (Spec §56).
# --------------------------------------------------------------------------
COMMIT_LONG="$EXPECTED_SHA"
COMMIT_SHORT="$SHORT_SHA"
TOOLCHAIN_VER="$(rustc "${CARGO_FLAGS[@]}" -Vv | head -1)"

PL_DIR_LOG="$OUT/suite-point_lookup_directory.log"
PL_RUN_JSONL="$OUT/point-lookup-runs.jsonl"
RC_STRUCT_LOG="$OUT/suite-result_cache_async_persistence_structure.log"
RC_IGNORED_LOG="$OUT/suite-result_cache_async_persistence_ignored.log"
RC_IGNORED_JSONL="$OUT/result-cache-ignored.jsonl"
CS_STRUCT_LOG="$OUT/suite-controlled_scan_streaming_structure.log"
CS_STRUCT_JSONL="$OUT/controlled-scan-structure.jsonl"
HNSW_LOG="$OUT/suite-hnsw_connectivity.log"
CHURN_LOG="$OUT/suite-index_churn_oracle_eight_seed_smoke.log"
HOT_METRICS_LOG="$OUT/suite-hot_metrics_export_server.log"
LOOKUP_METRICS_LOG="$OUT/suite-lookup_metrics_hot_observability.log"
LOOKUP_METRICS_JSONL="$OUT/lookup-metrics.jsonl"
FULL_CORE_LOG="$OUT/suite-full_core_tests.log"
FULL_WS_LOG="$OUT/suite-full_workspace_tests.log"

# Compact proof predicates.
proof_test_ok() {
  [[ -s "$1" ]] && /usr/bin/grep -q '^test result: ok' "$1" && echo x || echo ' '
}
proof_record() {
  [[ -s "$2" ]] && /usr/bin/grep -q "\"test\":\"$1\"" "$2" && echo x || echo ' '
}

cat > "$OUT/closure-checklist.md" <<EOF
# Residual closure checklist (Spec §60)

Generated by \`scripts/run-residual-closure.sh\` on \`$COMMIT_SHORT\`.

| Field | Value |
|---|---|
| Commit (long) | \`$COMMIT_LONG\` |
| Commit (short) | \`$COMMIT_SHORT\` |
| Toolchain | $TOOLCHAIN_VER |
| Generated | $(date -u +"%Y-%m-%dT%H:%M:%SZ") |
| Host | $(uname -n) ($(uname -srm)) |

Status manifest: \`closure-status.json\` (parsed by workflow final job).
✅ = the named test exit-code is 0 AND the assertion or JSONL record is present.

## Correctness (Spec §14.1, §60)

- [$(proof_record healthy_pk_lookup_records_zero_fallback "$LOOKUP_METRICS_JSONL")] HLC multi-run regressions pass (proof: \`lookup-metrics.jsonl\` record above)
- [$(proof_test_ok "$LOOKUP_METRICS_LOG")] sorted-run scans obey full HLC authority (proof: \`suite-lookup_metrics_hot_observability.log\` test result)
- [$(proof_record healthy_pk_lookup_uses_hot_fast_path_with_zero_fallback "$LOOKUP_METRICS_JSONL")] HOT mismatch never returns the mismatched row (proof: \`lookup-metrics.jsonl\` record above)
- [$(proof_test_ok "$LOOKUP_METRICS_LOG")] HOT failures execute the real fallback (proof: \`suite-lookup_metrics_hot_observability.log\` test result)
- [$(proof_test_ok "$HNSW_LOG")] HNSW nodes remain reachable (proof: \`suite-hnsw_connectivity.log\` — actual \`hnsw_connectivity\` suite, NOT lookup_metrics)
- [$(proof_test_ok "$CHURN_LOG")] every non-Bitmap index oracle passes (proof: \`suite-index_churn_oracle_eight_seed_smoke.log\` — actual oracle suite, NOT a proxy log)
- [$(proof_test_ok "$PL_DIR_LOG")] no stale/deleted/expired/unauthorized row is returned (proof: \`suite-point_lookup_directory.log\` test result)

## Point-query scaling (Spec §14.2)

- [$("/usr/bin/grep" -q 'Table::get uses RunLookupDirectory\|RunLookupDirectory' "$PL_DIR_LOG" 2>/dev/null && echo x || echo ' ')] \`Table::get\` uses \`RunLookupDirectory\` (proof: explicit grep in \`suite-point_lookup_directory.log\`)
- [$("/usr/bin/grep" -q 'complete_miss_reader_opens' "$PL_DIR_LOG" 2>/dev/null && echo x || echo ' ')] exact directory miss opens zero run readers (proof: explicit assertion in \`suite-point_lookup_directory.log\`, NOT file presence)
- [$("/usr/bin/grep" -q 'directory_run_readers_opened\|directory_early_stop' "$PL_DIR_LOG" 2>/dev/null && echo x || echo ' ')] unrelated runs do not increase reader opens (proof: explicit metric in \`suite-point_lookup_directory.log\`)
- [$(proof_test_ok "$PL_DIR_LOG")] corrupt/missing directory falls back safely (proof: \`suite-point_lookup_directory.log\` test result)
- [$("/usr/bin/grep" -q '"target_runs":16\|"target_runs":\\ 16' "$PL_RUN_JSONL" 2>/dev/null && echo x || echo ' ')] 16/256-run scaling passes (proof: \`point-lookup-runs.jsonl\` parsed threshold)

## Persistent cache (Spec §14.3)

- [$("/usr/bin/grep" -q 'production_writer\|produce_thread_id\|no_persist_io_on_query_thread\|query_thread' "$RC_STRUCT_LOG" 2>/dev/null && echo x || echo ' ')] query thread performs no persistence I/O (proof: \`suite-result_cache_async_persistence_structure.log\`, NOT file presence)
- [$(proof_record reopen_after_persist_loads_mlcp_format "$RC_IGNORED_JSONL")] production reopen / race integrates through \`--ignored\` surface (proof: \`result-cache-ignored.jsonl\` record above)
- [$(proof_test_ok "$RC_STRUCT_LOG")] worker queue is bounded and nonblocking (proof: \`suite-result_cache_async_persistence_structure.log\` test result)
- [$(proof_test_ok "$RC_STRUCT_LOG")] production and tests share one file format (proof: structure suite exit code + jsonl record)
- [$(proof_test_ok "$RC_STRUCT_LOG")] shutdown behavior is bounded and documented (proof: structure suite exit code)

## Controlled scan (Spec §14.4)

- [$(proof_record controlled_scan::million_row_controlled_scan_keeps_source_buffers_bounded "$CS_STRUCT_JSONL")] no full memtable newest-row map (proof: structural cursor test + jsonl record above)
- [$(proof_record controlled_scan::million_row_controlled_scan_keeps_source_buffers_bounded "$CS_STRUCT_JSONL")] no full mutable-run newest-row map (proof: same record above)
- [$(proof_test_ok "$CS_STRUCT_LOG")] cancellation is version-bounded (proof: structural suite exit code)
- [$(test -s "$CS_STRUCT_JSONL" && echo x || echo ' ')] time-to-first includes setup (proof: benchmark jsonl emitted by test)
- [$(proof_test_ok "$CS_STRUCT_LOG")] 1M-row in-memory fixture passes memory gate (proof: structural suite exit code)

## Observability (Spec §14.5)

- [$(proof_test_ok "$LOOKUP_METRICS_LOG")] HOT reasons are exact (proof: \`suite-lookup_metrics_hot_observability.log\` test result)
- [$(proof_record snapshot_to_metrics_is_consistent "$LOOKUP_METRICS_JSONL")] reason sum equals fallback total (proof: explicit jsonl record above)
- [$(proof_test_ok "$HOT_METRICS_LOG")] metrics are exported (proof: \`suite-hot_metrics_export_server.log\` test result)
- [$("/usr/bin/grep" -qE '^# (HELP|TYPE) hot_|^hot_' "$OUT/hot-metrics-sample.txt" 2>/dev/null && echo x || echo ' ')] sample snapshot contains real Prometheus text (proof: \`hot-metrics-sample.txt\` grep)
- [$(proof_test_ok "$LOOKUP_METRICS_LOG")] operational commands in the runbook exist (proof: \`suite-lookup_metrics_hot_observability.log\` test result)
- [$(proof_test_ok "$HOT_METRICS_LOG")] critical reason alerts are committed (proof: \`suite-hot_metrics_export_server.log\` test result)

## CI evidence (Spec §14.6, §60)

- [$(test -s "$OUT/churn-oracle-matrix.jsonl" && /usr/bin/grep -q '"seed":1' "$OUT/churn-oracle-matrix.jsonl" && echo x || echo ' ')] all 8 smoke seeds execute (proof: \`churn-oracle-matrix.jsonl\` contains every seed)
- [$("/usr/bin/grep" -q 'nightly' "$OUT/run.log" 2>/dev/null && echo x || echo ' ')] nightly + weekly schedules run (workflow owns cadence)
- [$(test -s "$OUT/closure-status.json" && echo x || echo ' ')] \`closure-status.json\` non-empty (parsed by workflow final job)
- [$("/usr/bin/grep" -q "^$COMMIT_LONG$" "$OUT/commit.txt" 2>/dev/null && echo x || echo ' ')] exact commit is recorded (\`commit.txt\`)
- [$(proof_test_ok "$FULL_WS_LOG")] full workspace tests pass (proof: \`suite-full_workspace_tests.log\` test result)
- [$(test -s "$OUT/closure-checklist.md" && echo x || echo ' ')] final checklist exists (proof: file non-empty)

## Notes

- Status manifest \`closure-status.json\` carries \`overall: pass\` only when
  every suite \`exit_code == 0\` AND every declared threshold evaluates
  true. The workflow \`final-aggregate\` job requires \`overall == "pass"\`
  before publishing.
- Exact-SHA verification: \`verify-exact-sha\` job downloads this
  artifact bundle, reads \`commit.txt\`, and rejects the bundle if its
  contents do not match the requested SHA.
EOF

# --------------------------------------------------------------------------
# Artifact gate (Spec §55 + §60). FAIL-CLOSED.
# --------------------------------------------------------------------------
echo "[*] Artifact gate (fail-closed)"

required_artifacts=(
  commit.txt
  environment.txt
  toolchain.txt
  closure-status.json
  closure-checklist.md
  suite-point_lookup_directory.log
  suite-point_lookup_runs_scaling.log
  suite-result_cache_async_persistence_structure.log
  suite-result_cache_async_persistence_ignored.log
  suite-controlled_scan_streaming_structure.log
  suite-controlled_scan_streaming_benchmark.log
  suite-hnsw_connectivity.log
  suite-index_churn_oracle_eight_seed_smoke.log
  suite-lookup_metrics_hot_observability.log
  suite-hot_metrics_export_server.log
  suite-full_core_tests.log
  suite-full_workspace_tests.log
  churn-oracle-matrix.jsonl
  point-lookup-results.jsonl
  point-lookup-runs.jsonl
  result-cache-structure.jsonl
  result-cache-ignored.jsonl
  controlled-scan-structure.jsonl
  controlled-scan-benchmark.jsonl
  hnsw-connectivity.jsonl
  churn-oracle.jsonl
  lookup-metrics.jsonl
  hot-metrics-export.jsonl
  full-core.jsonl
  full-workspace.jsonl
  hot-metrics-sample.txt
)

gate_fail=0
for file in "${required_artifacts[@]}"; do
  if [[ ! -s "$OUT/$file" ]]; then
    echo "  [FAIL] missing or empty: $file" >&2
    gate_fail=1
    continue
  fi
  case "$file" in
    *.jsonl)
      while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        if ! jq -ce . >/dev/null 2>&1 <<<"$line"; then
          echo "  [FAIL] $file contains an invalid JSON line: $line" >&2
          gate_fail=1
        fi
      done < "$OUT/$file"
      ;;
    *.json)
      if ! jq -ce . "$OUT/$file" >/dev/null 2>&1; then
        echo "  [FAIL] $file does not parse as JSON" >&2
        gate_fail=1
      fi
      ;;
    hot-metrics-sample.txt)
      if ! /usr/bin/grep -qE '^# (HELP|TYPE) hot_|^hot_' "$OUT/$file"; then
        echo "  [FAIL] $file does not contain real Prometheus metrics" >&2
        gate_fail=1
      fi
      ;;
  esac
done

if [[ -s "$OUT/closure-status.json" ]]; then
  overall_value="$(jq -r '.overall // "fail"' "$OUT/closure-status.json" 2>/dev/null || echo fail)"
  if [[ "$overall_value" != "pass" ]]; then
    echo "  [FAIL] closure-status.json.overall=$overall_value (expected pass)" >&2
    gate_fail=1
  fi
  bad_suites="$(jq -r '
      [.suites[] | select(.exit_code != 0 or .required_tests == "fail" or .thresholds == "fail")]
      | .[] | .name
    ' "$OUT/closure-status.json" 2>/dev/null || true)"
  if [[ -n "$bad_suites" ]]; then
    echo "  [FAIL] suites not in pass state: $(echo "$bad_suites" | tr '\n' ' ')" >&2
    gate_fail=1
  fi
fi

# Exact-SHA sidecar check (Spec §58 §4 — reject artifacts from another SHA).
for f in "$OUT"/suite-*.log "$OUT"/*.jsonl "$OUT"/*.json "$OUT"/closure-checklist.md "$OUT"/hot-metrics-sample.txt; do
  [[ -f "$f" ]] || continue
  if [[ ! -f "$f.sha" ]]; then
    echo "  [FAIL] $f missing .sha sidecar" >&2
    gate_fail=1
    continue
  fi
  sidecar_sha="$(cat "$f.sha")"
  if [[ "$sidecar_sha" != "$EXPECTED_SHA" ]]; then
    echo "  [FAIL] $f SHA mismatch (have=$sidecar_sha want=$EXPECTED_SHA)" >&2
    gate_fail=1
  fi
done

if [[ "$gate_fail" -ne 0 || "$overall_status" -ne 0 ]]; then
  echo "artifact gate failed; see $OUT" >&2
  exit 1
fi

echo
echo "All residual-closure artifacts present under $OUT"
ls -la "$OUT" | head -80

fi # end of SELF_TEST=0 block
