#!/usr/bin/env python3
"""Fail when aggregate release artifacts resolve a mixed MongrelDB train.

Engine packages (core/query) must match --expected (the engine release train).

Kit packages (mongreldb-kit / mongreldb-kit-core) are allowed to lag or lead
the engine: they must resolve to a single version each and agree across every
metadata file, but need not equal --expected. If both kit packages appear,
they must share that same kit train version.
"""

import argparse
import json
from pathlib import Path


ENGINE_COMPONENTS = (
    "mongreldb-core",
    "mongreldb-query",
)

KIT_COMPONENTS = (
    "mongreldb-kit",
    "mongreldb-kit-core",
)


def package_versions(packages, names):
    return {
        name: sorted({package["version"] for package in packages if package["name"] == name})
        for name in names
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata", nargs="+")
    parser.add_argument("--expected", required=True)
    args = parser.parse_args()

    failed = False
    kit_across_files = {name: set() for name in KIT_COMPONENTS}

    for filename in args.metadata:
        packages = json.loads(Path(filename).read_text(encoding="utf-8"))["packages"]
        engine = package_versions(packages, ENGINE_COMPONENTS)
        kit = package_versions(packages, KIT_COMPONENTS)

        for name, resolved in engine.items():
            if not resolved:
                # Engine may be absent from a pure-kit graph; only enforce when present.
                continue
            if resolved != [args.expected]:
                print(
                    f"{filename}: {name} resolved {resolved}, expected [{args.expected!r}]"
                )
                failed = True

        for name, resolved in kit.items():
            if not resolved:
                continue
            if len(resolved) != 1:
                print(f"{filename}: {name} resolved mixed versions {resolved}")
                failed = True
                continue
            kit_across_files[name].add(resolved[0])

        present_kit = {name: resolved[0] for name, resolved in kit.items() if resolved}
        if len(set(present_kit.values())) > 1:
            print(
                f"{filename}: kit train disagrees within file:"
                f" { {name: [ver] for name, ver in present_kit.items()} }"
            )
            failed = True

    for name, versions in kit_across_files.items():
        if len(versions) > 1:
            print(
                f"{name} disagrees across metadata files:"
                f" {sorted(versions)}"
            )
            failed = True

    present_trains = {
        name: next(iter(versions))
        for name, versions in kit_across_files.items()
        if versions
    }
    if len(set(present_trains.values())) > 1:
        print(f"kit train disagrees across components: {present_trains}")
        failed = True

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
