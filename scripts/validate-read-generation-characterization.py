#!/usr/bin/env python3
"""Validate the bounded 1M read-generation/write characterization."""

import json
import math
import sys
from pathlib import Path


def main():
    report = json.loads(Path(sys.argv[1]).read_text())
    thresholds = json.loads(Path(sys.argv[2]).read_text())
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    latency = report.get("commit_latency", {})
    stats = report.get("generation_stats_while_live", {})
    require(report.get("profile") == "release", "profile must be release")
    require(report.get("rows") == 1_000_000, "row count must be 1,000,000")
    require(report.get("writes", 0) >= 100, "at least 100 writes required")
    require(report.get("cursor_limit") == 32, "cursor limit must be 32")
    for name in ("p50_us", "p95_us", "p99_us"):
        value = latency.get(name)
        require(isinstance(value, (int, float)) and math.isfinite(value), f"{name} missing")
    require(
        latency.get("p99_us", math.inf) <= thresholds["max_commit_p99_us"],
        "commit p99 exceeds threshold",
    )
    require(
        report.get("peak_rss_bytes", math.inf) <= thresholds["max_peak_rss_bytes"],
        "peak RSS exceeds threshold",
    )
    require(stats.get("cow_clone_count") == 0, "whole-table COW clone detected")
    require(stats.get("estimated_cow_clone_bytes") == 0, "COW clone bytes detected")
    require(
        stats.get("active_read_generations", 0) <= report.get("cursor_limit", 0),
        "live generations exceed cursor bound",
    )
    require(
        stats.get("max_live_read_generations", 0) <= report.get("cursor_limit", 0),
        "historical generation peak exceeds cursor bound",
    )
    require(
        report.get("active_read_generations_after_drop") == 0,
        "generations remain live after cursor drop",
    )
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Read-generation characterization passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
