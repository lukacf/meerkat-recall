#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CARGO="${CARGO:-$ROOT/scripts/repo-cargo}"

packages=(
  elephant-types
  elephant-db
  elephant-policy
  elephant-storage
  elephant-extensions
  elephant-identity
  elephant-outbox
  elephant-embeddings
  elephant-migrations
  elephant-knowledge-views
  elephant
  elephant-pipeline
  elephant-mcp
  elephant-api
  elephant-pipeline-bin
)

for pkg in "${packages[@]}"; do
  echo "==> cargo package --allow-dirty -p ${pkg}"
  "$CARGO" package --allow-dirty -p "$pkg"
done
