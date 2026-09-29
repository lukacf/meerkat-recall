#!/usr/bin/env bash
# Rebuild the local Compose service without clearing shared Docker caches.
# Usage: ./rebuild-fast.sh [--full]
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
case "${1:-}" in
  "") docker compose build elephant ;;
  --full) docker compose build --no-cache elephant ;;
  *) echo "Usage: $0 [--full]" >&2; exit 2 ;;
esac
docker compose up -d elephant
docker compose ps
