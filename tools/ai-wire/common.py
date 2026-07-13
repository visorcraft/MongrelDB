#!/usr/bin/env python3
import json
import os
import platform
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

from fixtures import CONDITIONS, CREATE, ROWS

ROOT = Path(__file__).resolve().parents[2]
BASE_URL = os.environ.get("MONGRELDB_URL", "http://127.0.0.1:8080").rstrip("/")


def command(*args):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=True).stdout.strip()


def request(method, path, body=None):
    encoded = None if body is None else json.dumps(body, sort_keys=True).encode()
    req = urllib.request.Request(
        BASE_URL + path,
        data=encoded,
        method=method,
        headers={"content-type": "application/json"} if encoded else {},
    )
    try:
        with urllib.request.urlopen(req) as response:
            data = response.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        response = error.read().decode(errors="replace")
        raise RuntimeError(
            f"{method} {path} failed: HTTP {error.code}\nrequest={json.dumps(body, sort_keys=True)}\nresponse={response}"
        ) from error


def metadata(table):
    version = next(
        line.split('"')[1]
        for line in (ROOT / "crates/mongreldb-server/Cargo.toml").read_text().splitlines()
        if line.startswith("version =")
    )
    print(f"git_sha={command('git', 'rev-parse', 'HEAD')}")
    print(f"server_version={version}")
    print(f"rust_version={command('rustc', '--version')}")
    print(f"operating_system={platform.platform()}")
    print(f"build_profile={os.environ.get('MONGRELDB_PROFILE', 'release')}")
    print(f"features={os.environ.get('MONGRELDB_FEATURES', 'default')}")
    print(f"database_path={os.environ.get('MONGRELDB_DB_PATH', '<server-managed>')}")
    print(f"table={table}")


def run(kind):
    table = f"ai_wire_{kind}"
    metadata(table)
    request("GET", "/health")
    create = {"name": table, **CREATE}
    request("POST", "/kit/create_table", create)
    for cells in ROWS:
        request("POST", "/kit/txn", {"ops": [{"put": {"table": table, "cells": cells}}]})
    schema = request("GET", f"/kit/schema/{table}")
    assert len(schema["indexes"]) == 4, schema
    result = request(
        "POST",
        "/kit/query",
        {"table": table, "conditions": [CONDITIONS[kind]], "projection": [1]},
    )
    assert result["rows"][0]["cells"] == [1, 1], result
    print(json.dumps(result, sort_keys=True))
