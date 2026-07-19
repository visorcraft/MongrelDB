#!/usr/bin/env python3
"""Run one qualification command, tee output, and record elapsed milliseconds."""

import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: run-certification-command.py LOG COMMAND [ARG...]")
    log = Path(sys.argv[1])
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    with log.open("wb") as output:
        process = subprocess.Popen(
            sys.argv[2:],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        assert process.stdout is not None
        for chunk in iter(lambda: process.stdout.read(64 * 1024), b""):
            output.write(chunk)
            output.flush()
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
        status = process.wait()
    duration_ms = max(1, (time.monotonic_ns() - started) // 1_000_000)
    log.with_name(log.name + ".duration-ms").write_text(f"{duration_ms}\n")
    if status:
        raise SystemExit(status)


if __name__ == "__main__":
    main()
