#!/usr/bin/env python3
"""Fail closed when an AI release-qualification report is incomplete or weak."""

import argparse
import json
import math
import sys
from pathlib import Path


def nested(document, path):
    value = document
    for part in path.split("."):
        value = value[part]
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("thresholds", type=Path)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--skip-thresholds", action="store_true")
    args = parser.parse_args()

    report = json.loads(args.report.read_text())
    thresholds = json.loads(args.thresholds.read_text())
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    require(report.get("git_sha") == args.expected_sha, "git_sha does not match expected SHA")
    require(report.get("git_dirty") is False, "git_dirty must be false")
    require(report.get("qualification_mode") is True, "qualification_mode must be true")
    require(report.get("profile") == "release", "profile must be release")
    expected_rows = args.expected_rows or thresholds["rows"]
    require(report.get("rows") == expected_rows, "row count does not match expected corpus")
    require(report.get("queries", 0) >= thresholds["minimum_queries"], "too few measured queries")
    try:
        require(
            nested(report, "checkpoint_inspection.status") == "ok",
            "checkpoint inspection did not succeed",
        )
    except (KeyError, TypeError):
        errors.append("missing checkpoint_inspection.status")
    require(report.get("checkpoint_bytes", 0) > 0, "checkpoint is empty")

    payloads = report.get("index_payloads", [])
    payload_kinds = {payload.get("kind") for payload in payloads if payload.get("payload_bytes", 0) > 0}
    for kind in ("hot_primary", "bitmap", "ann", "sparse", "minhash"):
        require(kind in payload_kinds, f"missing non-empty {kind} checkpoint payload")

    expected_rerank = {10, 50, 100, 200}
    try:
        rerank = nested(report, "ann.exact_rerank")
    except (KeyError, TypeError):
        rerank = []
        errors.append("missing ann.exact_rerank")
    require({entry.get("candidate_k") for entry in rerank} == expected_rerank, "exact rerank candidate matrix is incomplete")
    for entry in rerank:
        require(entry.get("final_k") == 10, "exact rerank final_k must be 10")
        for field in ("hamming_recall_at_10", "cosine_recall_at_10", "p50_us", "p95_us"):
            value = entry.get(field)
            require(
                isinstance(value, (int, float)) and math.isfinite(value),
                f"exact rerank {entry.get('candidate_k')} {field} must be finite",
            )

    checks = [
        ("ann.p95_us", lambda value: value <= thresholds["ann"]["maximum_p95_us"]),
        ("ann.hamming_recall_at_10", lambda value: value >= thresholds["ann"]["minimum_hamming_recall_at_10"]),
        ("ann.cosine_recall_at_10", lambda value: value >= thresholds["ann"]["minimum_cosine_recall_at_10"]),
        ("sparse.p95_us", lambda value: value <= thresholds["sparse"]["maximum_p95_us"]),
        ("minhash.p95_us", lambda value: value <= thresholds["minhash"]["maximum_p95_us"]),
        ("minhash.candidate_recall_at_10", lambda value: value >= thresholds["minhash"]["minimum_candidate_recall_at_10"]),
        ("hybrid.p95_us", lambda value: value <= thresholds["hybrid"]["maximum_p95_us"]),
    ]
    if not args.skip_thresholds:
        for path, passes in checks:
            try:
                value = nested(report, path)
                require(isinstance(value, (int, float)) and math.isfinite(value), f"{path} must be finite")
                if isinstance(value, (int, float)) and math.isfinite(value):
                    require(passes(value), f"{path} failed threshold: {value}")
            except (KeyError, TypeError):
                errors.append(f"missing {path}")

    def inspect_finite(value, path="report"):
        if isinstance(value, float) and not math.isfinite(value):
            errors.append(f"{path} is not finite")
        elif isinstance(value, dict):
            for key, child in value.items():
                inspect_finite(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                inspect_finite(child, f"{path}[{index}]")

    inspect_finite(report)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("AI benchmark qualification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
