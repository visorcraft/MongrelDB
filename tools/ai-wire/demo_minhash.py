#!/usr/bin/env python3
import json

from common import ROOT, request, run

golden = json.loads((ROOT / "docs/ai/minhash-v1-golden.json").read_text())
assert len(golden) == 6

run("minhash")
fixture_rows = {}
for index, fixture in enumerate(golden, start=100):
    logical_id = 1000 + index
    request(
        "POST",
        "/kit/txn",
        {
            "ops": [
                {
                    "put": {
                        "table": "ai_wire_minhash",
                        "cells": [
                            1,
                            logical_id,
                            2,
                            "published",
                            3,
                            [[index, 1.0]],
                            4,
                            [1, -1, 1, -1, 1, -1, 1, -1],
                            5,
                            [fixture["member"]],
                        ],
                    }
                }
            ]
        },
    )
    row = request(
        "POST",
        "/kit/query",
        {
            "table": "ai_wire_minhash",
            "conditions": [{"pk": {"value": logical_id}}],
            "projection": [1],
        },
    )["rows"][0]
    fixture_rows[row["row_id"]] = fixture["member"]
for physical_row_id, member in fixture_rows.items():
    result = request(
        "POST",
        "/kit/retrieve",
        {
            "table": "ai_wire_minhash",
            "retriever": {
                "min_hash": {"column_id": 5, "members": [member], "k": 20}
            },
        },
    )
    assert any(hit["row_id"] == physical_row_id for hit in result["hits"]), (member, result)
