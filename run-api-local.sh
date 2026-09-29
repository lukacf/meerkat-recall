#!/bin/bash
# Local development script for running elephant-api outside Docker

set -euo pipefail

# SurrealDB connection (Docker container)
export SURREALDB_URL=ws://localhost:8000
export SURREALDB_NAMESPACE=elephant
export SURREALDB_DATABASE=test
export SURREALDB_USER=root
export SURREALDB_PASS=root

# API server config
export ELEPHANT_HOST=127.0.0.1
export ELEPHANT_PORT=3000
export SKIP_AUTH=true

# Logging
export RUST_LOG=info,elephant_api=debug

echo "Starting elephant-api with local SurrealDB..."
echo "SurrealDB: $SURREALDB_URL"
echo "API: http://$ELEPHANT_HOST:$ELEPHANT_PORT"
echo ""

cargo run --release -p elephant-api --features "elephant-api/meerkat-extraction"
