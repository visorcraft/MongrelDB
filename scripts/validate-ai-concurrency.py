#!/usr/bin/env python3
"""Validate the scored read/write concurrency characterization shape."""

import json
import math
import sys
from pathlib import Path


def main():
    report = json.loads(Path(sys.argv[1]).read_text())
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    require(report.get("profile") == "release", "profile must be release")
    require(report.get("rls") is True, "RLS must be enabled")
    require(report.get("exact_vector_rerank") is True, "exact rerank must be enabled")
    scenarios = report.get("scenarios", [])
    expected = {(readers, writers) for readers in (1, 4, 16, 32) for writers in (0, 1, 4)}
    require(
        {(entry.get("readers"), entry.get("writers")) for entry in scenarios} == expected,
        "reader/writer matrix is incomplete",
    )
    for scenario in scenarios:
        readers = scenario.get("readers", 0)
        writers = scenario.get("writers", 0)
        query = scenario.get("query_latency", {})
        commit = scenario.get("commit_latency", {})
        require(query.get("count", 0) > 0, f"{readers}/{writers} has no query samples")
        require(
            commit.get("count", 0) > 0 if writers else commit.get("count") == 0,
            f"{readers}/{writers} has wrong commit samples",
        )
        for name, metrics in (("query", query), ("commit", commit)):
            if metrics.get("count", 0) == 0:
                continue
            for percentile in ("p50_us", "p95_us", "p99_us"):
                value = metrics.get(percentile)
                require(
                    isinstance(value, (int, float)) and math.isfinite(value),
                    f"{readers}/{writers} {name} {percentile} must be finite",
                )
        require(
            isinstance(scenario.get("throughput_ops_per_second"), (int, float))
            and math.isfinite(scenario["throughput_ops_per_second"])
            and scenario["throughput_ops_per_second"] > 0,
            f"{readers}/{writers} throughput must be finite and positive",
        )
        require(scenario.get("peak_rss_bytes", 0) > 0, f"{readers}/{writers} peak RSS missing")
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("AI concurrency characterization passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
