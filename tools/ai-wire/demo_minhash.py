#!/usr/bin/env python3
import json

from common import ROOT, request, run

golden = json.loads((ROOT / "docs/ai/minhash-v1-golden.json").read_text())
assert len(golden) == 6

run("minhash")
fixture_rows = {}
for index, fixture in enumerate(golden, start=100):
    row_id = 1000 + index
    fixture_rows[row_id] = fixture["member"]
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
                            row_id,
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
for row_id, member in fixture_rows.items():
    result = request(
        "POST",
        "/kit/query",
        {
            "table": "ai_wire_minhash",
            "conditions": [
                {
                    "minhash_similar_members": {
                        "column_id": 5,
                        "members": [member],
                        "k": 20,
                    }
                }
            ],
            "projection": [1],
        },
    )
    assert any(row["cells"] == [1, row_id] for row in result["rows"]), (member, result)
