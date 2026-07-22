#!/usr/bin/env python3
"""Create certification evidence only after every required CI log exists.

Architecture task IDs follow the Stage 0–5 architecture specification
(FND-*, S1*, S2*, S3*, S4*, S5*). Residual R1–R10 IDs are emitted as optional
aliases for backward compatibility.

Status rule (audit §2.4 / P0.9): Integrated ≠ Qualified. This generator marks
every Stage task Integrated unless real multi-class evidence paths are
configured for that task. It does NOT flip Integrated → Qualified without
exact-SHA product evidence of the required classes.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


TESTS = {
    "component_versions": (
        "python3 scripts/verify-component-versions",
        "component-versions.log",
    ),
    "format": ("cargo fmt --all -- --check", "cargo-fmt.log"),
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
    "fuzz_smoke": ("cargo +nightly-2026-07-16 fuzz run", "fuzz-smoke.log"),
    "crash_matrix": (
        "cargo test -p mongreldb-core --test fault_injection --test crash --test crash_process",
        "crash-matrix.log",
    ),
    "packaged_artifact_conformance": (
        "bash scripts/qualify-packaged-artifacts.sh qualified-artifacts.tar.gz",
        "packaged-artifact-test.log",
    ),
}

# Mandatory Stage 0–5 architecture task IDs (must stay aligned with
# crates/mongreldb-core/src/certification.rs::MANDATORY_ARCHITECTURE_TASK_IDS).
MANDATORY_ARCHITECTURE_TASKS = [
    # Stage 0
    "FND-001",
    "FND-002",
    "FND-003",
    "FND-004",
    "FND-005",
    "FND-006",
    "FND-007",
    # Stage 1
    "S1A-001",
    "S1A-002",
    "S1A-003",
    "S1A-004",
    "S1B-001",
    "S1B-002",
    "S1B-003",
    "S1B-004",
    "S1B-005",
    "S1C-001",
    "S1C-002",
    "S1C-003",
    "S1C-004",
    "S1D-001",
    "S1D-002",
    "S1D-003",
    "S1D-004",
    "S1D-005",
    "S1D-006",
    "S1D-007",
    "S1E-001",
    "S1E-002",
    "S1E-003",
    "S1E-004",
    "S1F-001",
    "S1F-002",
    "S1F-003",
    "S1G",
    # Stage 2
    "S2A-001",
    "S2A-002",
    "S2B-001",
    "S2B-002",
    "S2B-003",
    "S2B-004",
    "S2C",
    "S2D",
    "S2E",
    "S2F",
    "S2G",
    "S2H",
    # Stage 3
    "S3A",
    "S3B",
    "S3C",
    "S3D",
    "S3E",
    "S3F",
    "S3G",
    "S3H",
    "S3I",
    "S3J",
    "S3K",
    "S3L",
    # Stage 4
    "S4A",
    "S4B",
    "S4C",
    "S4D",
    "S4E",
    "S4F",
    "S4G",
    # Stage 5
    "S5A",
    "S5B",
    "S5C",
    "S5D",
    "S5E",
    "S5F",
]

# Optional residual aliases (previous audit R1–R10). Never substitute for Stage IDs.
RESIDUAL_ALIAS_TASKS = [f"R{index}" for index in range(1, 11)]

# Related CI evidence for traceability only. Listing evidence here does NOT
# promote a task to Qualified; status stays Integrated until a future gate
# proves multi-class product evidence for an exact SHA.
RELATED_EVIDENCE = {
    "FND-001": ("workspace_tests",),
    "FND-002": ("component_versions", "workspace_tests"),
    "FND-003": ("workspace_tests",),
    "FND-004": ("workspace_tests",),
    "FND-005": ("workspace_tests",),
    "FND-006": ("crash_matrix", "workspace_tests"),
    "FND-007": ("workspace_tests", "client_tests"),
    "S1A-001": ("workspace_tests",),
    "S1A-002": ("workspace_tests",),
    "S1A-003": ("workspace_tests",),
    "S1A-004": ("workspace_tests", "server_tests"),
    "S1B-001": ("workspace_tests",),
    "S1B-002": ("workspace_tests",),
    "S1B-003": ("workspace_tests",),
    "S1B-004": ("workspace_tests", "crash_matrix"),
    "S1B-005": ("workspace_tests",),
    "S1C-001": ("workspace_tests",),
    "S1C-002": ("workspace_tests",),
    "S1C-003": ("workspace_tests", "ai_benchmark"),
    "S1C-004": ("workspace_tests",),
    "S1D-001": ("workspace_tests", "server_tests", "client_tests"),
    "S1D-002": ("workspace_tests", "server_tests", "client_tests"),
    "S1D-003": ("workspace_tests", "server_tests"),
    "S1D-004": ("server_tests", "client_tests"),
    "S1D-005": ("server_tests",),
    "S1D-006": ("server_tests", "client_tests"),
    "S1D-007": ("server_tests",),
    "S1E-001": ("workspace_tests", "server_tests"),
    "S1E-002": ("workspace_tests", "server_tests"),
    "S1E-003": ("workspace_tests", "server_tests"),
    "S1E-004": ("workspace_tests",),
    "S1F-001": ("workspace_tests",),
    "S1F-002": ("workspace_tests",),
    "S1F-003": ("workspace_tests",),
    "S1G": ("workspace_tests", "server_tests", "crash_matrix"),
    "S2A-001": ("workspace_tests",),
    "S2A-002": ("workspace_tests", "server_tests"),
    "S2B-001": ("workspace_tests",),
    "S2B-002": ("workspace_tests", "crash_matrix"),
    "S2B-003": ("workspace_tests",),
    "S2B-004": ("workspace_tests",),
    "S2C": ("workspace_tests", "server_tests"),
    "S2D": ("workspace_tests",),
    "S2E": ("workspace_tests",),
    "S2F": ("workspace_tests",),
    "S2G": ("workspace_tests", "client_tests"),
    "S2H": ("workspace_tests",),
    "S3A": ("workspace_tests",),
    "S3B": ("workspace_tests",),
    "S3C": ("workspace_tests",),
    "S3D": ("workspace_tests",),
    "S3E": ("workspace_tests",),
    "S3F": ("workspace_tests",),
    "S3G": ("workspace_tests",),
    "S3H": ("workspace_tests",),
    "S3I": ("workspace_tests",),
    "S3J": ("workspace_tests",),
    "S3K": ("workspace_tests",),
    "S3L": ("workspace_tests",),
    "S4A": ("workspace_tests", "server_tests"),
    "S4B": ("workspace_tests", "server_tests"),
    "S4C": ("workspace_tests", "ai_benchmark"),
    "S4D": ("workspace_tests", "ai_concurrency"),
    "S4E": ("workspace_tests",),
    "S4F": ("workspace_tests",),
    "S4G": ("workspace_tests",),
    "S5A": ("mysql_wire_client_compat", "mysql_snapshot_binlog"),
    "S5B": (
        "client_tests",
        "node_tests",
        "node_smoke",
        "ffi_tests",
        "kit_ffi_tests",
        "jni_build",
    ),
    "S5C": ("workspace_tests", "server_tests"),
    "S5D": ("workspace_tests", "server_tests"),
    "S5E": ("workspace_tests", "server_tests"),
    "S5F": tuple(TESTS),
    # Residual aliases (traceability only; not Stage qualification).
    "R1": ("workspace_tests", "workspace_release_tests"),
    "R2": (
        "workspace_tests",
        "server_tests",
        "client_tests",
        "packaged_artifact_conformance",
    ),
    "R3": ("workspace_tests", "server_tests", "packaged_artifact_conformance"),
    "R4": (
        "mysql_wire_client_compat",
        "mysql_snapshot_binlog",
        "packaged_artifact_conformance",
    ),
    "R5": ("workspace_tests", "workspace_release_tests"),
    "R6": ("workspace_tests", "server_tests"),
    "R7": ("fuzz_smoke", "crash_matrix", "packaged_artifact_conformance"),
    "R8": ("format", "workspace_tests", "packaged_artifact_conformance"),
    "R9": (
        "workspace_tests",
        "server_tests",
        "client_tests",
        "packaged_artifact_conformance",
    ),
    "R10": tuple(TESTS),
}

# Tasks that may be marked Qualified only when explicit multi-class product
# evidence is configured here. Empty by default: no Integrated→Qualified flip
# without exact-SHA product evidence (P0.9-X5).
#
# Shape: task_id -> {"evidence": [...test ids...], "evidence_classes": [...]}
# evidence_classes use snake_case names matching EvidenceClass in certification.rs.
QUALIFIED_TASK_EVIDENCE: dict[str, dict] = {}

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
    # Refuse to generate a manifest that would claim architecture Qualified
    # via the residual matrix alone.
    if any(status == "Qualified" for status in rows.values()):
        raise SystemExit(
            "implementation-status.md must not mark residual R1–R10 Qualified "
            "without Stage 0–5 exact-SHA evidence (P0.9)"
        )
    return hashlib.sha256(text.encode()).hexdigest()


def architecture_task_entry(task_id: str) -> dict:
    """Emit a task row. Default status is Integrated (never auto-Qualified)."""
    qualified = QUALIFIED_TASK_EVIDENCE.get(task_id)
    if qualified:
        evidence = list(qualified["evidence"])
        evidence_classes = list(qualified["evidence_classes"])
        missing = [item for item in evidence if item not in TESTS]
        if missing:
            raise SystemExit(
                f"qualified evidence for {task_id} references unknown tests: "
                f"{', '.join(missing)}"
            )
        if not evidence or not evidence_classes:
            raise SystemExit(
                f"qualified evidence for {task_id} requires evidence and evidence_classes"
            )
        return {
            "id": task_id,
            "status": "qualified",
            "evidence": evidence,
            "evidence_classes": evidence_classes,
        }
    related = list(RELATED_EVIDENCE.get(task_id, ()))
    # Related evidence is traceability only while status remains Integrated.
    return {
        "id": task_id,
        "status": "integrated",
        "evidence": related,
        "evidence_classes": [],
    }


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

    architecture_tasks = [
        architecture_task_entry(task_id) for task_id in MANDATORY_ARCHITECTURE_TASKS
    ]
    # Residual R1–R10 aliases for backward compatibility (optional extras).
    architecture_tasks.extend(
        architecture_task_entry(task_id) for task_id in RESIDUAL_ALIAS_TASKS
    )

    # Safety: never emit Qualified unless explicitly configured above.
    if any(task["status"] == "qualified" for task in architecture_tasks):
        if not QUALIFIED_TASK_EVIDENCE:
            raise SystemExit("internal error: Qualified task without configured evidence")

    manifest = {
        "commit": commit,
        "artifact_sha256": hashlib.sha256(args.artifact.read_bytes()).hexdigest(),
        "implementation_status_sha256": implementation_status_sha256(),
        "rust_version": rust_version,
        "architecture_tasks": architecture_tasks,
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
