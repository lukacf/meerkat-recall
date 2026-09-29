#!/bin/bash
# Local development script for running elephant-pipeline outside Docker

set -euo pipefail

# SurrealDB connection (Docker container)
export SURREALDB_URL=ws://localhost:8000
export SURREALDB_NAMESPACE=elephant
export SURREALDB_DATABASE=test
export SURREALDB_USER=root
export SURREALDB_PASS=root

# Pipeline workers
export PIPELINE_INGEST_ENABLED=true
export PIPELINE_EMBED_ENABLED=true
export PIPELINE_PROMOTE_ENABLED=true
export PIPELINE_INGEST_POLL_INTERVAL=5
export PIPELINE_EMBED_POLL_INTERVAL=5
export PIPELINE_PROMOTE_POLL_INTERVAL=5

# Embedding config
export PIPELINE_EMBED_PROVIDER=openai
export PIPELINE_EMBED_MODEL=text-embedding-3-large
export PIPELINE_EMBED_DIMS=3072

# Agentic extractor (API must also be running locally for MCP endpoint)
export PIPELINE_AGENTIC_MCP_URL=http://127.0.0.1:3000/mcp/agent
export PIPELINE_AGENTIC_MODEL=claude-sonnet-4-20250514

# Logging
export RUST_LOG=info,elephant_pipeline=debug

# Optional: budget limits
# export PIPELINE_BUDGET_DAILY_ITEMS_GRAPH=100

echo "Starting elephant-pipeline with local SurrealDB..."
echo "SurrealDB: $SURREALDB_URL"
echo "MCP URL: $PIPELINE_AGENTIC_MCP_URL"
echo ""

cargo run --release -p elephant-pipeline-bin --features "elephant-pipeline/meerkat-extraction"
