#!/usr/bin/env bash
# Bump the MongrelDB workspace version everywhere it's pinned: the shared
# workspace.package.version (covers mongreldb-core/mongreldb-query via
# `version.workspace = true`), each standalone crate's own version
# (mongreldb-client/-perf/-server/-node) plus their internal path-dependency
# pins on mongreldb-core/mongreldb-query, and the Node addon's
# package.json/package-lock.json. Then regenerates every Cargo.lock (root
# workspace + each standalone crate) so the bump is fully reflected.
#
# Usage: scripts/bump-version.sh NEW_VERSION
# Example: scripts/bump-version.sh 0.19.5
#
# This script only edits files and regenerates lockfiles -- it does not
# commit, tag, or push. See AGENTS.md "Releases" for the full flow.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NEW="${1:?usage: scripts/bump-version.sh NEW_VERSION}"
if ! [[ "$NEW" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '$NEW' doesn't look like semver (X.Y.Z)" >&2
  exit 1
fi

OLD="$(grep -m1 '^version = ' Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
if [[ "$NEW" == "$OLD" ]]; then
  echo "error: $NEW is already the current version" >&2
  exit 1
fi
echo "Bumping mongreldb $OLD -> $NEW"

# Every Cargo.toml carrying this workspace's own version and/or a path-dep
# pin on mongreldb-core/mongreldb-query. Add new standalone crates here.
CARGO_FILES=(
  Cargo.toml
  crates/mongreldb-query/Cargo.toml
  crates/mongreldb-client/Cargo.toml
  crates/mongreldb-perf/Cargo.toml
  crates/mongreldb-server/Cargo.toml
  crates/mongreldb-node/Cargo.toml
)
for f in "${CARGO_FILES[@]}"; do
  sed -i "s/version = \"$OLD\"/version = \"$NEW\"/g" "$f"
done
sed -i "s/\"version\": \"$OLD\"/\"version\": \"$NEW\"/g" \
  crates/mongreldb-node/package.json crates/mongreldb-node/package-lock.json

echo "Regenerating lockfiles (this compiles each crate; can take a while)..."
cargo check --workspace --all-features >/dev/null
for d in mongreldb-client mongreldb-server mongreldb-perf mongreldb-node; do
  echo "  crates/$d"
  (cd "crates/$d" && cargo check >/dev/null)
done

# Safety net: catch any file the hardcoded list above missed (e.g. a new
# crate). Warns rather than fails -- Cargo.lock/target/node_modules always
# mention the old version transitively and are expected here.
STRAY="$(grep -rl "\"$OLD\"" --include="*.toml" --include="*.json" . 2>/dev/null \
  | grep -v -E "/target/|node_modules|Cargo\.lock" || true)"
if [[ -n "$STRAY" ]]; then
  echo "warning: these files still mention $OLD -- check whether they need the bump too:" >&2
  echo "$STRAY" >&2
fi

cat <<EOF

Done. Review with 'git diff', then run the release gate before committing:
  cargo fmt --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace --all-features

Then, per AGENTS.md "Releases":
  git commit -am "release $NEW"
  git tag -a v$NEW -m "v$NEW — <one-line summary>"
  git push origin master && git push origin v$NEW
CI publishes to npm and crates.io automatically on the tag push.
EOF
