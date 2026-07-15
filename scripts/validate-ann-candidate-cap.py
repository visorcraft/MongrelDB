#!/usr/bin/env python3
"""Validate the >250k ANN candidate-cap memory characterization."""

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

    cap = report.get("raw_candidate_cap", 0)
    require(report.get("profile") == "release", "profile must be release")
    require(report.get("rows", 0) > cap >= 250_000, "index must exceed raw cap")
    require(report.get("rls_selectivity", 1) < 0.01, "RLS selectivity must be below 1%")
    require(report.get("ann_candidate_cap_hit") is True, "cap-hit trace missing")
    require(report.get("rls_rows_evaluated", math.inf) <= cap, "adaptive breadth exceeded cap")
    require(report.get("available_authorized_hit_returned") is True, "authorized hit missing")
    require(report.get("exact_rerank_applied") is True, "exact rerank missing")
    require(report.get("oom_or_failure") is False, "qualification reported failure")
    require(
        0 < report.get("peak_rss_bytes", math.inf) <= thresholds["max_peak_rss_bytes"],
        "peak RSS exceeds threshold",
    )
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("ANN candidate-cap characterization passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
