#!/usr/bin/env bash
# Builds a PGO-optimized mongreldb-server binary (PLAN.md work-stream B):
#   1. instrumented build with -Cprofile-generate
#   2. profile collection on the P0 + P2 benchmark workloads
#   3. llvm-profdata merge
#   4. optimized rebuild with -Cprofile-use
#
# The instrumented build goes into a scratch target dir under the profile
# dir; the final PGO build lands in the workspace's normal target dir, so
# the optimized binary is at target/release/mongreldb-server. Note that the
# profile-use RUSTFLAGS change makes cargo rebuild the workspace there.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROFILE_DIR=${PGO_PROFILE_DIR:-"$ROOT/target/pgo"}
# Packaged deployments opt into these features (release.yml, Dockerfile), so
# the profile is collected against the same code layout as shipped binaries.
FEATURES=${PGO_FEATURES:-"cluster,native-rpc,oidc,vault-kms,remote-embedding"}
SKIP_COLLECT=0
DRY_RUN=0

usage() {
    cat <<EOF
usage: $0 [--profile-dir DIR] [--features LIST] [--skip-collect] [--dry-run]

options:
  --profile-dir DIR  profile output dir holding profraw/, merged.profdata and
                     the instrumented scratch target (default: $PROFILE_DIR,
                     or \$PGO_PROFILE_DIR)
  --features LIST    mongreldb-server features to build/profile (default:
                     "$FEATURES", or \$PGO_FEATURES; pass "" for standalone)
  --skip-collect     reuse an existing DIR/merged.profdata: skip the
                     instrumented build and benchmark collection, rebuild only
  --dry-run          print the commands without executing them
  -h, --help         show this help

LLVM_PROFDATA overrides the llvm-profdata binary; by default it is located
under the rustc sysroot (rustup component add llvm-tools-preview).
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --profile-dir) PROFILE_DIR=${2:?--profile-dir requires a value}; shift 2 ;;
        --features) FEATURES=${2?--features requires a value}; shift 2 ;;
        --skip-collect) SKIP_COLLECT=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h | --help) usage; exit 0 ;;
        *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

PROFRAW_DIR="$PROFILE_DIR/profraw"
MERGED="$PROFILE_DIR/merged.profdata"
INSTR_TARGET="$PROFILE_DIR/target-instrumented"

# llvm-profdata ships with the llvm-tools-preview rustup component, under the
# toolchain sysroot (see PLAN.md B.2).
LLVM_PROFDATA=${LLVM_PROFDATA:-"$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/host: //p')/bin/llvm-profdata"}
if [ "$DRY_RUN" -eq 0 ] && [ ! -x "$LLVM_PROFDATA" ]; then
    echo "error: llvm-profdata not found at $LLVM_PROFDATA" >&2
    echo "install it with: rustup component add llvm-tools-preview" >&2
    exit 1
fi

run() {
    echo "+ $*"
    if [ "$DRY_RUN" -eq 0 ]; then
        "$@"
    fi
}

features_args=()
if [ -n "$FEATURES" ]; then
    features_args=(--features "$FEATURES")
fi

if [ "$SKIP_COLLECT" -eq 1 ]; then
    if [ "$DRY_RUN" -eq 0 ] && [ ! -f "$MERGED" ]; then
        echo "error: --skip-collect given but $MERGED does not exist" >&2
        exit 1
    fi
    echo "reusing existing profile: $MERGED"
else
    # B.1 — instrumented build (fresh profraw dir so stale runs never merge).
    run rm -rf "$PROFRAW_DIR"
    run mkdir -p "$PROFRAW_DIR"
    run env RUSTFLAGS="-Cprofile-generate=$PROFRAW_DIR" CARGO_TARGET_DIR="$INSTR_TARGET" \
        cargo +stable build --release -p mongreldb-server --tests "${features_args[@]}"

    # B.2 — profile collection. Each workload runs under the same
    # -Cprofile-generate flags so its counters land in $PROFRAW_DIR.
    # Primary: the P2 benchmark (1,000 loopback point queries).
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "+ $INSTR_TARGET/release/deps/scale_test-* loopback_point_query_p95_baseline --nocapture"
    else
        scale_bin=""
        shopt -s nullglob
        for candidate in "$INSTR_TARGET/release/deps"/scale_test-*; do
            if [ -f "$candidate" ] && [ -x "$candidate" ]; then
                scale_bin=$candidate
                break
            fi
        done
        shopt -u nullglob
        if [ -z "$scale_bin" ]; then
            echo "error: no instrumented scale_test binary under $INSTR_TARGET/release/deps" >&2
            exit 1
        fi
        run "$scale_bin" loopback_point_query_p95_baseline --nocapture
    fi
    # Secondary: the P0 write-path bench (create + put hot path).
    run env RUSTFLAGS="-Cprofile-generate=$PROFRAW_DIR" CARGO_TARGET_DIR="$INSTR_TARGET" \
        cargo bench -p mongreldb-core --bench write_path -- --sample-size 100 --measurement-time 3
    # Tertiary: the core qualification suite (embedded point query + scan).
    run env RUSTFLAGS="-Cprofile-generate=$PROFRAW_DIR" CARGO_TARGET_DIR="$INSTR_TARGET" \
        cargo test -p mongreldb-core --test qualification --release -- --nocapture

    # B.2 — merge the raw profile data.
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "+ $LLVM_PROFDATA merge -o $MERGED $PROFRAW_DIR/*.profraw"
    else
        shopt -s nullglob
        profraws=("$PROFRAW_DIR"/*.profraw)
        shopt -u nullglob
        if [ ${#profraws[@]} -eq 0 ]; then
            echo "error: no .profraw files collected in $PROFRAW_DIR" >&2
            exit 1
        fi
        run "$LLVM_PROFDATA" merge -o "$MERGED" "${profraws[@]}"
    fi
fi

# B.3 — optimized rebuild using the merged profile. Lands in the workspace's
# default target dir (target/release/mongreldb-server).
run env RUSTFLAGS="-Cprofile-use=$MERGED" \
    cargo +stable build --release -p mongreldb-server --tests "${features_args[@]}"

echo "PGO build complete: $ROOT/target/release/mongreldb-server (profile: $MERGED)"
