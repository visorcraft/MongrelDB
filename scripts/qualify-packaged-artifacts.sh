#!/usr/bin/env bash
set -euo pipefail

archive=${1:?qualified artifact archive is required}
root=${2:-.}
work=$(mktemp -d)
trap 'test -z "${server_pid:-}" || kill "$server_pid" 2>/dev/null || true; rm -rf "$work"' EXIT

tar -xzf "$archive" -C "$work"
test -x "$work/mongreldb-server"
test -f "$work/libmongreldb.so"

cc \
  -std=c11 -D_GNU_SOURCE -Wall -Wextra -Werror \
  -o "$work/c-smoke" \
  "$root/crates/mongreldb-ffi/tests/c_test.c" \
  -I "$root/crates/mongreldb-ffi/include" \
  -L "$work" -lmongreldb \
  -Wl,-rpath,"$work" \
  -lpthread -ldl -lm
"$work/c-smoke"

port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
"$work/mongreldb-server" "$work/database" --port "$port" >"$work/server.log" 2>&1 &
server_pid=$!
for _ in {1..100}; do
  if curl --fail --silent "http://127.0.0.1:$port/health" >/dev/null; then
    kill "$server_pid"
    wait "$server_pid" || true
    server_pid=
    echo "Packaged server and C ABI conformance passed."
    exit 0
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    sed -n '1,200p' "$work/server.log"
    exit 1
  fi
  sleep 0.1
done
sed -n '1,200p' "$work/server.log"
exit 1
