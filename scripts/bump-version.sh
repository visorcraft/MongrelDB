#!/usr/bin/env bash
# Bump the MongrelDB workspace version everywhere it's pinned: the shared
# workspace.package.version (covers workspace-versioned crates), each
# explicitly-versioned release crate plus their internal
# path-dependency pins on mongreldb-core/mongreldb-query, the Node addon's
# package.json/package-lock.json, the JNI pom.xml, the README JAR filenames,
# and the ffi-release workflow JAR filenames. Then regenerates the root and
# standalone Cargo.lock files so the bump is fully reflected.
#
# Note: external crates.io deps (mongreldb-kit / mongreldb-kit-core) are NOT
# bumped — those track the separate mongreldb-kit repo's release cycle.
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
# pin on mongreldb-core/mongreldb-query. Add new release crates here.
# Note: FFI/JNI/kit-ffi crates depend on mongreldb-kit from
# crates.io (e.g. "0.48") — those pins are intentionally NOT bumped here
# because they track the separate mongreldb-kit repo's release cycle.
CARGO_FILES=(
  Cargo.toml
  crates/mongreldb-types/Cargo.toml
  crates/mongreldb-log/Cargo.toml
  crates/mongreldb-fault/Cargo.toml
  crates/mongreldb-sim/Cargo.toml
  crates/mongreldb-protocol/Cargo.toml
  crates/mongreldb-core/Cargo.toml
  crates/mongreldb-query/Cargo.toml
  crates/mongreldb-consensus/Cargo.toml
  crates/mongreldb-cluster/Cargo.toml
  crates/mongreldb-client/Cargo.toml
  crates/mongreldb-perf/Cargo.toml
  crates/mongreldb-server/Cargo.toml
  crates/mongreldb-node/Cargo.toml
  crates/mongreldb-ffi/Cargo.toml
  crates/mongreldb-kit-ffi/Cargo.toml
  crates/mongreldb-jni/Cargo.toml
  crates/mongreldb-migrate-mysql/Cargo.toml
  crates/mongreldb-mysql-wire/Cargo.toml
)
for f in "${CARGO_FILES[@]}"; do
  sed -i "s/version = \"$OLD\"/version = \"$NEW\"/g" "$f"
done
sed -i "s/\"version\": \"$OLD\"/\"version\": \"$NEW\"/g" \
  crates/mongreldb-node/package.json crates/mongreldb-node/package-lock.json

# JNI Maven pom.xml version.
sed -i "s|<version>$OLD</version>|<version>$NEW</version>|g" \
  crates/mongreldb-jni/java/pom.xml

# README and ffi-release workflow JAR filenames (mongreldb-jni-VERSION-*.jar).
sed -i "s/mongreldb-jni-$OLD/mongreldb-jni-$NEW/g" \
  README.md .github/workflows/ffi-release.yml

# C FFI smoke test build-info assertions (engine_version/query_version).
sed -i "s/\\\\\"engine_version\\\\\":\\\\\"$OLD\\\\\"/\\\\\"engine_version\\\\\":\\\\\"$NEW\\\\\"/; \
        s/\\\\\"query_version\\\\\":\\\\\"$OLD\\\\\"/\\\\\"query_version\\\\\":\\\\\"$NEW\\\\\"/" \
  crates/mongreldb-ffi/tests/c_test.c

echo "Regenerating the workspace lockfile (this can take a while)..."
cargo check --workspace --all-features >/dev/null
cargo metadata --manifest-path crates/mongreldb-kit-ffi/Cargo.toml --format-version 1 >/dev/null
cargo metadata --manifest-path crates/mongreldb-jni/Cargo.toml --format-version 1 >/dev/null

# Safety net: catch any file the hardcoded list above missed (e.g. a new
# crate). Warns rather than fails -- Cargo.lock/target/node_modules always
# mention the old version transitively and are expected here.
STRAY="$(grep -rl "\"$OLD\"" \
  --include="*.toml" --include="*.json" --include="*.xml" \
  --include="*.yml" --include="*.yaml" --include="*.md" \
  . 2>/dev/null \
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

After crates.io contains $NEW, refresh the standalone aggregate locks and
commit them before dispatching ffi-release.yml from master:
  cargo update --manifest-path crates/mongreldb-jni/Cargo.toml -p mongreldb-core --precise $NEW
  cargo update --manifest-path crates/mongreldb-jni/Cargo.toml -p mongreldb-query --precise $NEW
  cargo update --manifest-path crates/mongreldb-kit-ffi/Cargo.toml -p mongreldb-core --precise $NEW
  cargo update --manifest-path crates/mongreldb-kit-ffi/Cargo.toml -p mongreldb-query --precise $NEW
EOF
