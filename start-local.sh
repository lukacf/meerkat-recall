#!/bin/bash
# Start API and Pipeline locally for fast development
# Usage: source .envrc && ./start-local.sh

set -euo pipefail

# Check required env vars
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "ERROR: ANTHROPIC_API_KEY not set"
    exit 1
fi

echo "Starting elephant-supervisor locally..."
echo "SurrealDB: ${SURREALDB_URL}"
echo "API will be on: http://${ELEPHANT_HOST}:${ELEPHANT_PORT}"
echo ""

exec cargo run --release -p elephant-supervisor
