#!/usr/bin/env python3
import json

from common import ROOT, run

golden = json.loads((ROOT / "docs/ai/minhash-v1-golden.json").read_text())
assert len(golden) >= 6

run("minhash")
