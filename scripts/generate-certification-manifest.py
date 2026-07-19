#!/usr/bin/env python3
"""Create certification evidence only after every required CI log exists."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


TESTS = {
    "format": ("cargo fmt --check", "cargo-fmt.log"),
    "clippy": (
        "cargo clippy --workspace --all-targets --all-features -- -D warnings",
        "cargo-clippy.log",
    ),
    "workspace_tests": (
        "cargo test --workspace --all-features",
        "cargo-test.log",
    ),
    "workspace_release_tests": (
        "cargo test --workspace --release --all-features",
        "cargo-test-release.log",
    ),
    "server_tests": (
        "cargo test --manifest-path crates/mongreldb-server/Cargo.toml --all-targets --all-features",
        "server-test.log",
    ),
    "client_tests": (
        "cargo test --manifest-path crates/mongreldb-client/Cargo.toml --all-targets --all-features",
        "client-test.log",
    ),
    "mysql_wire_client_compat": (
        "cargo test --manifest-path crates/mongreldb-mysql-wire/Cargo.toml --all-targets",
        "mysql-wire-test.log",
    ),
    "mysql_snapshot_binlog": (
        "cargo test --manifest-path crates/mongreldb-migrate-mysql/Cargo.toml --test mysql_container -- --nocapture",
        "mysql-migrate-test.log",
    ),
    "node_tests": ("npm test", "node-test.log"),
    "node_smoke": ("node smoke.mjs", "node-smoke.log"),
    "ffi_tests": (
        "cargo test --manifest-path crates/mongreldb-ffi/Cargo.toml --release",
        "ffi-test.log",
    ),
    "ffi_sanitizer": (
        "MONGRELDB_C_SANITIZE=1 cargo test --manifest-path crates/mongreldb-ffi/Cargo.toml --release --test c_smoke_test",
        "ffi-sanitizer.log",
    ),
    "kit_ffi_tests": (
        "cargo test --manifest-path crates/mongreldb-kit-ffi/Cargo.toml --release",
        "kit-ffi-test.log",
    ),
    "jni_build": (
        "cargo build --manifest-path crates/mongreldb-jni/Cargo.toml --release",
        "jni-build.log",
    ),
    "ai_benchmark": (
        "python3 scripts/validate-ai-benchmark.py",
        "ai-benchmark-validation.log",
    ),
    "ai_concurrency": (
        "python3 scripts/validate-ai-concurrency.py",
        "ai-concurrency-validation.log",
    ),
    "fuzz_smoke": ("cargo +nightly fuzz run", "fuzz-smoke.log"),
    "crash_matrix": (
        "cargo test -p mongreldb-core --test fault_injection --test crash --test crash_process",
        "crash-matrix.log",
    ),
    "packaged_artifact_conformance": (
        "bash scripts/qualify-packaged-artifacts.sh qualified-artifacts.tar.gz",
        "packaged-artifact-test.log",
    ),
}

IMPLEMENTATION_STATUS = Path("docs/architecture/implementation-status.md")


def implementation_status_sha256() -> str:
    text = IMPLEMENTATION_STATUS.read_text()
    rows = {
        cells[1].strip().split()[0]: cells[2].strip()
        for line in text.splitlines()
        if line.startswith("| R")
        and len(cells := line.split("|")) >= 3
    }
    required = {f"R{index}" for index in range(1, 11)}
    if rows.keys() != required:
        missing = ", ".join(sorted(required - rows.keys()))
        extra = ", ".join(sorted(rows.keys() - required))
        raise SystemExit(
            f"implementation status rows mismatch; missing: {missing or 'none'}; "
            f"extra: {extra or 'none'}"
        )
    allowed = {"Not Started", "Scaffolded", "Integrated", "Qualified"}
    invalid = [f"{item}={status}" for item, status in rows.items() if status not in allowed]
    if invalid:
        raise SystemExit(f"invalid implementation status: {', '.join(invalid)}")
    return hashlib.sha256(text.encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit")
    args = parser.parse_args()

    commit = args.commit or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    checkout_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    if commit != checkout_commit:
        raise SystemExit(
            f"certification commit {commit} does not match checkout {checkout_commit}"
        )
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise SystemExit("certification checkout is dirty")
    rust_version = subprocess.check_output(["rustc", "--version"], text=True).strip()
    missing = [
        name
        for _, name in TESTS.values()
        if not (args.evidence_dir / name).is_file()
        or not (args.evidence_dir / name).stat().st_size
    ]
    if missing:
        raise SystemExit(f"missing or empty certification evidence: {', '.join(missing)}")
    if not args.artifact.is_file() or not args.artifact.stat().st_size:
        raise SystemExit(f"missing or empty qualified artifact: {args.artifact}")
    durations = {}
    for test_id, (_, evidence) in TESTS.items():
        duration_path = args.evidence_dir / f"{evidence}.duration-ms"
        try:
            duration = int(duration_path.read_text().strip())
        except (FileNotFoundError, ValueError):
            raise SystemExit(f"missing or invalid duration evidence: {duration_path}")
        if duration <= 0:
            raise SystemExit(f"non-positive duration evidence: {duration_path}")
        durations[test_id] = duration

    manifest = {
        "commit": commit,
        "artifact_sha256": hashlib.sha256(args.artifact.read_bytes()).hexdigest(),
        "implementation_status_sha256": implementation_status_sha256(),
        "rust_version": rust_version,
        "tests": [
            {
                "id": test_id,
                "command": command,
                "status": "passed",
                "duration_ms": durations[test_id],
                "artifact": evidence,
            }
            for test_id, (command, evidence) in TESTS.items()
        ],
    }
    args.output.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
