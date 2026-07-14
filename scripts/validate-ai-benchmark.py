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
    try:
        bounded = nested(report, "bounded_window")
        expected_union = min(expected_rows, 100_000)
        require(bounded.get("union_size") == expected_union, "bounded-window union is incomplete")
        require(bounded.get("hits") == 10, "bounded-window final hit count must be 10")
        require(
            bounded.get("projection_rows", expected_union) <= 20,
            "bounded-window projected more than the ranked refill window",
        )
        require(
            bounded.get("projection_cells", expected_union) <= 20,
            "bounded-window decoded more than the ranked refill window",
        )
    except (KeyError, TypeError):
        errors.append("missing bounded_window")
    try:
        clean = nested(report, "qualification_profiles.clean")
        operational = nested(report, "qualification_profiles.operational")
        require(clean.get("rows") == expected_rows, "clean profile row count is incomplete")
        require(clean.get("updates") == 0 and clean.get("deletes") == 0, "clean profile is mutated")
        require(operational.get("rows") == expected_rows, "operational profile row count is incomplete")
        require(operational.get("updates") == expected_rows // 20, "operational profile must apply 5% updates")
        require(operational.get("deletes") == expected_rows // 100, "operational profile must apply 1% deletes")
        require(operational.get("ttl") is True, "operational profile must enable TTL")
        require(operational.get("immutable_runs", 0) > 1, "operational profile must contain multiple runs")
        require(operational.get("hot_memtable_rows", 0) > 0, "operational profile must retain a hot memtable")
        require(operational.get("mutable_run_rows", 0) > 0, "operational profile must retain a mutable run")
        multi_tenant = nested(report, "qualification_profiles.multi_tenant")
        require(multi_tenant.get("rows") == expected_rows, "multi-tenant profile row count is incomplete")
        require(multi_tenant.get("column_masks", 0) > 0, "multi-tenant profile must configure a column mask")
        rls_profiles = multi_tenant.get("profiles", [])
        require(
            {entry.get("selectivity") for entry in rls_profiles} == {0.01, 0.10, 0.50},
            "multi-tenant profile selectivity matrix is incomplete",
        )
        for entry in rls_profiles:
            require(entry.get("hits") == 10, "multi-tenant profile must return ten authorized hits")
            require(entry.get("rows_evaluated", 0) > 0, "multi-tenant profile did not evaluate RLS")
            require(
                entry.get("policy_columns_decoded") == entry.get("rows_evaluated"),
                "multi-tenant profile decoded unrelated policy columns",
            )
        encrypted = nested(report, "qualification_profiles.encrypted")
        require(encrypted.get("enabled") is True, "encrypted profile must run in qualification")
        require(encrypted.get("rows", 0) > 0, "encrypted profile is empty")
        require(encrypted.get("hits", 0) > 0, "encrypted profile retrieval returned no hits")
        realistic = nested(report, "qualification_profiles.realistic")
        require(realistic.get("rows", 0) > 0, "realistic profile is empty")
        require(
            realistic.get("exact_rerank_recall_at_4") == 1.0,
            "realistic profile exact-rerank recall must be 1.0",
        )
        relevance = nested(report, "qualification_profiles.relevance")
        require(relevance.get("documents", 0) >= 15, "relevance corpus has too few documents")
        require(relevance.get("passages", 0) >= 100, "relevance corpus has too few passages")
        require(relevance.get("queries", 0) >= 15, "relevance suite has too few queries")
        require(relevance.get("index_size_bytes", 0) > 0, "relevance index is empty")
        for mode in ("dense_only", "sparse_only", "rrf", "rrf_exact_vector_rerank"):
            metrics = relevance.get(mode, {})
            for metric in (
                "recall_at_10",
                "mrr_at_10",
                "ndcg_at_10",
                "answer_context_coverage_at_10",
                "duplicate_suppression",
                "p50_us",
                "p95_us",
            ):
                value = metrics.get(metric)
                require(
                    isinstance(value, (int, float)) and math.isfinite(value),
                    f"relevance {mode} {metric} must be finite",
                )
            require(
                metrics.get("duplicate_suppression") == 1.0,
                f"relevance {mode} returned duplicate rows",
            )
    except (KeyError, TypeError):
        errors.append("missing qualification profile")

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
