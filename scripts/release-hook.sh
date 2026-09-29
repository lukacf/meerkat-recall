#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:?Usage: release-hook.sh <version>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

SENTINEL="$ROOT/.release-hook-done"
if [[ -f "$SENTINEL" ]] && [[ "$(cat "$SENTINEL")" == "$VERSION" ]]; then
  exit 0
fi

cd "$ROOT"

echo "==> Release hook: validating Elephant release state for $VERSION"
bash "$ROOT/scripts/verify-version-parity.sh"

git add \
  "$ROOT/Cargo.toml" \
  "$ROOT/Cargo.lock" \
  "$ROOT/CHANGELOG.md" \
  "$ROOT/bin/elephant-api/Cargo.toml" \
  "$ROOT/bin/elephant/Cargo.toml" \
  "$ROOT/bin/elephant-pipeline/Cargo.toml" \
  "$ROOT/crates/elephant/Cargo.toml" \
  "$ROOT/crates/types/Cargo.toml" \
  "$ROOT/crates/elephant-db/Cargo.toml" \
  "$ROOT/crates/storage/Cargo.toml" \
  "$ROOT/crates/policy/Cargo.toml" \
  "$ROOT/crates/identity/Cargo.toml" \
  "$ROOT/crates/outbox/Cargo.toml" \
  "$ROOT/crates/embeddings/Cargo.toml" \
  "$ROOT/crates/pipeline/Cargo.toml" \
  "$ROOT/crates/mcp/Cargo.toml" \
  "$ROOT/crates/extensions/Cargo.toml" \
  "$ROOT/crates/migrations/Cargo.toml" \
  "$ROOT/crates/knowledge-views/Cargo.toml"

echo "$VERSION" > "$SENTINEL"
echo "==> Release hook complete"
