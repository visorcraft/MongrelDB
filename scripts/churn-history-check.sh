#!/usr/bin/env bash
# churn-history-check.sh — REM-H durable churn history closure gate
# (Spec §12.3 / §12.5).
#
# Queries the GitHub API (via the `gh` CLI) for the preceding scheduled runs
# of the churn workflows and enforces the durable success history required
# for closure:
#
#   - 30 consecutive nightly passes  (.github/workflows/index-churn-nightly.yml)
#   - 4 consecutive weekly passes    (.github/workflows/index-churn-weekly.yml)
#
# Every counted run must:
#   - be a scheduled run on the expected branch (workflow_dispatch runs do
#     not extend history);
#   - have concluded "success" (in-progress/null conclusions fail closed);
#   - carry the expected, non-expired result artifact
#     (`index-churn-nightly-result` / `index-churn-weekly-result`);
#   - trace the expected branch/SHA lineage: when --expected-sha is given,
#     the newest counted run's head_sha must be an ancestor of (or equal to)
#     that SHA per the GitHub compare API.
#
# Reruns of the same SHA do NOT count as independent history (Spec §12.3):
# runs are deduplicated by head_sha, and any failed run — including a failed
# rerun — inside the considered window fails the gate.
#
# The script fails closed: any `gh`/API error, malformed payload, missing
# run, non-success conclusion, missing/expired artifact, or lineage mismatch
# exits nonzero.
#
# Usage:
#   bash scripts/churn-history-check.sh [--branch BRANCH] [--repo OWNER/REPO]
#                                       [--expected-sha SHA]
#                                       [--nightly N] [--weekly M]
#   bash scripts/churn-history-check.sh --self-test [fixture-dir]
#
# Defaults: --repo comes from `gh repo view`, --branch from the repository
# default branch, --nightly 30, --weekly 4.
#
# --self-test generates stubbed API fixtures (no network, no `gh`) and
# proves the gate fails on missing runs, failed runs, missing artifacts,
# wrong branch lineage, diverged SHA lineage, same-SHA reruns, and API
# errors — and passes on a compliant 30-nightly/4-weekly history.
#
# ----------------------------------------------------------------------------
# Churn-oracle env contract honored by the workflows (the Rust test binary
# crates/mongreldb-core/tests/index_churn_oracle.rs is owned by the REM-F/G
# track; these knobs are the contract it must implement for REM-H):
#
#   Existing (already implemented at the base commit):
#     MONGRELDB_ORACLE_SEED                 one seed per test process
#     MONGRELDB_ORACLE_OPERATIONS           ops per family per seed
#                                           (nightly 10000, weekly 30000)
#
#   Required by the REM-H workflows (values "1"/"0" unless noted):
#     MONGRELDB_ORACLE_ENCRYPTION           include encrypted lifecycle ops
#     MONGRELDB_ORACLE_TTL                  include TTL expiry ops
#     MONGRELDB_ORACLE_HISTORICAL_SNAPSHOTS include pinned-snapshot reads
#     MONGRELDB_ORACLE_CANDIDATE_CAP_PRESSURE drive candidate caps hard
#     MONGRELDB_ORACLE_WORK_BUDGET_PRESSURE exhaust retrieval work budgets
#     MONGRELDB_ORACLE_LIFECYCLE_OPS        reopen/rebuild/compaction at full
#                                           op-matrix weight
#     MONGRELDB_ORACLE_WEEKLY_PROFILE       1 = enable the weekly profile
#     MONGRELDB_ORACLE_STALE_CANDIDATE_RATIO stale:live candidate ratio (100)
#     MONGRELDB_ORACLE_HOT_KEY_HISTORY      ops of history per hot key (512)
#     MONGRELDB_ORACLE_COMPACTION_CYCLES    explicit compaction cycles (8)
#     MONGRELDB_ORACLE_REOPEN_CYCLES        explicit close+reopen cycles (4)
#     MONGRELDB_ORACLE_METRICS_JSON         path; write RSS/latency capture
#                                           (p50/p95/p99 per phase) as JSON
#     MONGRELDB_ORACLE_FAILURE_DIR          path; persist failing DB
#                                           directories + full op logs
#
#   Record names the test must emit via emit_metric! (coordination contract):
#     index_churn_oracle::family::fm
#     index_churn_oracle::family::learned_range
#     index_churn_oracle::family::ann_hnsw_dense
#     index_churn_oracle::family::ann_hnsw_binary_sign
#     index_churn_oracle::family::ann_product_quantization
#     index_churn_oracle::family::ann_diskann_dense
#     index_churn_oracle::family::ann_ivf_dense
#     index_churn_oracle::family::sparse
#     index_churn_oracle::family::minhash
#     index_churn_oracle::seed_determinism
# ----------------------------------------------------------------------------

set -euo pipefail

NIGHTLY_WORKFLOW="index-churn-nightly.yml"
WEEKLY_WORKFLOW="index-churn-weekly.yml"
NIGHTLY_ARTIFACT="index-churn-nightly-result"
WEEKLY_ARTIFACT="index-churn-weekly-result"

BRANCH=""
REPO=""
EXPECTED_SHA=""
REQUIRED_NIGHTLY=30
REQUIRED_WEEKLY=4
SELF_TEST=0
SELF_TEST_DIR=""

# When set (self-test recursion only), the API layer reads stubbed fixtures
# from this directory instead of calling `gh`.
FIXTURE_DIR="${CHURN_HISTORY_FIXTURE_DIR:-}"

usage() {
  sed -n '2,60p' "${BASH_SOURCE[0]}"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# --------------------------------------------------------------------------
# GitHub API access layer (stubbed by fixtures in self-test recursion).
# --------------------------------------------------------------------------

api_list_runs() {
  # $1 = workflow file, $2 = branch -> runs JSON payload
  local workflow=$1 branch=$2
  if [[ -n "$FIXTURE_DIR" ]]; then
    cat "$FIXTURE_DIR/runs-$workflow.json"
    return
  fi
  gh api "repos/$REPO/actions/workflows/$workflow/runs?branch=$branch&event=schedule&per_page=100"
}

api_list_artifacts() {
  # $1 = run id -> artifacts JSON payload
  local run_id=$1
  if [[ -n "$FIXTURE_DIR" ]]; then
    cat "$FIXTURE_DIR/artifacts-$run_id.json"
    return
  fi
  gh api "repos/$REPO/actions/runs/$run_id/artifacts?per_page=100"
}

api_compare_status() {
  # $1 = base sha, $2 = head sha -> compare status string
  local base=$1 head=$2
  if [[ -n "$FIXTURE_DIR" ]]; then
    jq -r '.status' "$FIXTURE_DIR/compare-$base-$head.json"
    return
  fi
  gh api "repos/$REPO/compare/$base...$head" --jq '.status'
}

# --------------------------------------------------------------------------
# Core gate: require N consecutive distinct-SHA passes for one workflow.
# --------------------------------------------------------------------------

require_consecutive() {
  local label=$1 workflow=$2 artifact=$3 required=$4
  echo "checking $label history: $workflow (require $required consecutive passes, artifact '$artifact')"

  local runs_json
  if ! runs_json=$(api_list_runs "$workflow" "$BRANCH"); then
    fail "$label: could not list scheduled runs for $workflow on branch '$BRANCH' (API error — failing closed)"
  fi
  if ! jq -e '.workflow_runs | type == "array"' >/dev/null 2>&1 <<<"$runs_json"; then
    fail "$label: malformed runs payload for $workflow (parse error — failing closed)"
  fi

  local total
  total=$(jq '.workflow_runs | length' <<<"$runs_json")

  local count=0 newest_sha="" i run_id sha branch conclusion artifacts
  declare -A seen=()
  for ((i = 0; i < total && count < required; i++)); do
    run_id=$(jq -r ".workflow_runs[$i].id" <<<"$runs_json")
    sha=$(jq -r ".workflow_runs[$i].head_sha" <<<"$runs_json")
    branch=$(jq -r ".workflow_runs[$i].head_branch" <<<"$runs_json")
    conclusion=$(jq -r ".workflow_runs[$i].conclusion // \"null\"" <<<"$runs_json")

    if [[ "$branch" != "$BRANCH" ]]; then
      fail "$label: run $run_id targets branch '$branch' (expected '$BRANCH') — wrong lineage"
    fi
    if [[ "$conclusion" != "success" ]]; then
      fail "$label: run $run_id (sha $sha) concluded '$conclusion', expected 'success'"
    fi
    if ! artifacts=$(api_list_artifacts "$run_id"); then
      fail "$label: could not list artifacts for run $run_id (API error — failing closed)"
    fi
    if ! jq -e --arg a "$artifact" \
      '[.artifacts[]? | select(.name == $a and ((.expired // false) | not))] | length > 0' \
      >/dev/null 2>&1 <<<"$artifacts"; then
      fail "$label: run $run_id (sha $sha) is missing the required non-expired artifact '$artifact'"
    fi

    if [[ -z "${seen[$sha]:-}" ]]; then
      seen[$sha]=1
      count=$((count + 1))
      if [[ -z "$newest_sha" ]]; then
        newest_sha=$sha
      fi
    else
      echo "  note: run $run_id is a rerun of already-counted sha $sha; not independent history"
    fi
  done

  if [[ $count -lt $required ]]; then
    fail "$label: only $count consecutive distinct-SHA passes found (required $required)"
  fi

  if [[ -n "$EXPECTED_SHA" ]]; then
    local status
    if ! status=$(api_compare_status "$newest_sha" "$EXPECTED_SHA"); then
      fail "$label: could not compare $newest_sha...$EXPECTED_SHA (API error — failing closed)"
    fi
    case "$status" in
      ahead | identical) ;;
      *)
        fail "$label: newest counted run sha $newest_sha is not an ancestor of expected sha $EXPECTED_SHA (compare status '$status')"
        ;;
    esac
  fi

  echo "OK: $label — $count consecutive distinct-SHA passes with artifact '$artifact'"
}

# --------------------------------------------------------------------------
# Self-test fixture generators.
# --------------------------------------------------------------------------

ST_EXPECTED_SHA="ffffffffffffffffffffffffffffffffffffffff"

st_runs() {
  # $1 dir, $2 workflow, $3 count, $4 branch, $5 id base, $6 artifact name,
  # $7 fail_at index (-1 none), $8 dup_at index (-1 none),
  # $9 wrong_branch_at index (-1 none), ${10} missing_artifact_at index (-1 none)
  local dir=$1 workflow=$2 count=$3 branch=$4 idbase=$5 artifact=$6
  local fail_at=$7 dup_at=$8 wrong_at=$9 missing_art_at=${10}
  local arr='[]' i id sha concl br
  for ((i = 0; i < count; i++)); do
    id=$((idbase + i))
    sha=$(printf '%040x' $((i + 1)))
    if [[ $i -eq $dup_at ]]; then
      # Rerun of the previous (older) run: same head_sha.
      sha=$(printf '%040x' $((i + 2)))
    fi
    concl="success"
    if [[ $i -eq $fail_at ]]; then
      concl="failure"
    fi
    br=$branch
    if [[ $i -eq $wrong_at ]]; then
      br="feature-not-the-expected-branch"
    fi
    arr=$(jq -c --argjson id "$id" --arg sha "$sha" --arg br "$br" --arg cc "$concl" \
      '. + [{id: $id, head_branch: $br, head_sha: $sha, conclusion: $cc, event: "schedule"}]' \
      <<<"$arr")
    if [[ $i -eq $missing_art_at ]]; then
      jq -n '{total_count: 1, artifacts: [{name: "unrelated-artifact", expired: false}]}' \
        >"$dir/artifacts-$id.json"
    else
      jq -n --arg a "$artifact" \
        '{total_count: 1, artifacts: [{name: $a, expired: false}]}' \
        >"$dir/artifacts-$id.json"
    fi
  done
  jq -n --argjson runs "$arr" '{total_count: ($runs | length), workflow_runs: $runs}' \
    >"$dir/runs-$workflow.json"
}

st_compare() {
  # $1 dir, $2 base sha, $3 head sha, $4 status
  local dir=$1 base=$2 head=$3 status=$4
  jq -n --arg s "$status" '{status: $s}' >"$dir/compare-$base-$head.json"
}

# --------------------------------------------------------------------------
# Self-test battery.
# --------------------------------------------------------------------------

run_self_test() {
  local dir=$1
  mkdir -p "$dir"
  local failures=0

  st_case() {
    # $1 scenario name, $2 expected exit code
    local name=$1 expected=$2
    local sdir="$dir/$name"
    local rc=0
    if CHURN_HISTORY_FIXTURE_DIR="$sdir" bash "${BASH_SOURCE[0]}" \
      --branch master --repo visorcraft/MongrelDB \
      --expected-sha "$ST_EXPECTED_SHA" >"$sdir/output.log" 2>&1; then
      rc=0
    else
      rc=$?
    fi
    if [[ $rc -eq $expected ]]; then
      echo "  [ OK ] $name (exit $rc, expected $expected)"
    else
      echo "  [FAIL] $name (exit $rc, expected $expected)" >&2
      sed 's/^/         /' "$sdir/output.log" >&2 || true
      failures=$((failures + 1))
    fi
  }

  local newest_sha
  newest_sha=$(printf '%040x' 1)

  echo "churn-history-check self-test (fixtures: $dir)"

  # 1. Compliant history: 30 nightly + 4 weekly distinct-SHA passes -> pass.
  local s="$dir/compliant"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" -1 -1 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case compliant 0

  # 2. Missing runs: only 12 nightly passes -> fail.
  s="$dir/missing_runs"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 12 master 1000 "$NIGHTLY_ARTIFACT" -1 -1 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case missing_runs 1

  # 3. Failed run inside the nightly window -> fail.
  s="$dir/failed_run"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" 5 -1 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case failed_run 1

  # 4. Wrong branch lineage on one run -> fail.
  s="$dir/wrong_branch"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" -1 -1 3 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case wrong_branch 1

  # 5. Missing expected artifact on one weekly run -> fail.
  s="$dir/missing_artifact"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" -1 -1 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case missing_artifact 1

  # 6. Same-SHA rerun: 30 successes but only 29 distinct SHAs -> fail
  #    (reruns never extend history, Spec §12.3).
  s="$dir/rerun_dedup"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" -1 2 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case rerun_dedup 1

  # 7. Diverged SHA lineage (newest counted sha not an ancestor) -> fail.
  s="$dir/diverged_lineage"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" -1 -1 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" diverged
  st_case diverged_lineage 1

  # 8. API error (missing runs fixture) -> fail closed.
  s="$dir/api_error"
  mkdir -p "$s"
  st_runs "$s" "$WEEKLY_WORKFLOW" 4 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case api_error 1

  # 9. Weekly history too short (3 of 4) -> fail.
  s="$dir/short_weekly"
  mkdir -p "$s"
  st_runs "$s" "$NIGHTLY_WORKFLOW" 30 master 1000 "$NIGHTLY_ARTIFACT" -1 -1 -1 -1
  st_runs "$s" "$WEEKLY_WORKFLOW" 3 master 2000 "$WEEKLY_ARTIFACT" -1 -1 -1 -1
  st_compare "$s" "$newest_sha" "$ST_EXPECTED_SHA" ahead
  st_case short_weekly 1

  if [[ $failures -gt 0 ]]; then
    echo "FAIL: $failures self-test scenario(s) misbehaved" >&2
    exit 1
  fi
  echo "OK: all self-test scenarios behaved as expected"
  exit 0
}

# --------------------------------------------------------------------------
# Argument parsing + main.
# --------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test | self-test)
      SELF_TEST=1
      if [[ $# -gt 1 && "$2" != --* ]]; then
        SELF_TEST_DIR=$2
        shift
      fi
      shift
      ;;
    --branch)
      BRANCH=$2
      shift 2
      ;;
    --repo)
      REPO=$2
      shift 2
      ;;
    --expected-sha)
      EXPECTED_SHA=$2
      shift 2
      ;;
    --nightly)
      REQUIRED_NIGHTLY=$2
      shift 2
      ;;
    --weekly)
      REQUIRED_WEEKLY=$2
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ $SELF_TEST -eq 1 ]]; then
  run_self_test "${SELF_TEST_DIR:-$(mktemp -d)}"
fi

command -v jq >/dev/null 2>&1 || fail "jq is required"
if [[ -z "$FIXTURE_DIR" ]]; then
  command -v gh >/dev/null 2>&1 || fail "the gh CLI is required (or run --self-test)"
  if [[ -z "$REPO" ]]; then
    if ! REPO=$(gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>/dev/null); then
      fail "could not resolve the repository via 'gh repo view' (pass --repo OWNER/REPO)"
    fi
  fi
  if [[ -z "$BRANCH" ]]; then
    if ! BRANCH=$(gh api "repos/$REPO" --jq '.default_branch' 2>/dev/null); then
      fail "could not resolve the default branch via the GitHub API (pass --branch)"
    fi
  fi
fi

if [[ -z "$BRANCH" ]]; then
  fail "--branch is required"
fi

echo "churn-history-check: repo=$REPO branch=$BRANCH nightly>=$REQUIRED_NIGHTLY weekly>=$REQUIRED_WEEKLY"
if [[ -n "$EXPECTED_SHA" ]]; then
  echo "churn-history-check: expected sha lineage $EXPECTED_SHA"
fi

require_consecutive "nightly" "$NIGHTLY_WORKFLOW" "$NIGHTLY_ARTIFACT" "$REQUIRED_NIGHTLY"
require_consecutive "weekly" "$WEEKLY_WORKFLOW" "$WEEKLY_ARTIFACT" "$REQUIRED_WEEKLY"

echo "PASS: durable churn history satisfies the REM-H closure gate"
