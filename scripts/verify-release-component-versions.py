#!/usr/bin/env python3
"""Fail when aggregate release artifacts resolve a mixed MongrelDB train."""

import argparse
import json
from pathlib import Path


COMPONENTS = (
    "mongreldb-core",
    "mongreldb-query",
    "mongreldb-kit",
    "mongreldb-kit-core",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata", nargs="+")
    parser.add_argument("--expected", required=True)
    args = parser.parse_args()

    failed = False
    for filename in args.metadata:
        packages = json.loads(Path(filename).read_text(encoding="utf-8"))["packages"]
        versions = {
            name: sorted({package["version"] for package in packages if package["name"] == name})
            for name in COMPONENTS
        }
        for name, resolved in versions.items():
            if resolved != [args.expected]:
                print(f"{filename}: {name} resolved {resolved}, expected [{args.expected!r}]")
                failed = True
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
