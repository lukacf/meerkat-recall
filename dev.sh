#!/bin/bash
# Fast development workflow
# Runs SurrealDB in Docker, API+pipeline locally with auto-rebuild

set -euo pipefail

MODE="${1:-watch}"  # watch | run | test

# Start SurrealDB if not running
if ! docker ps | grep -q elephant-surrealdb-dev; then
    echo "Starting SurrealDB..."
    docker compose -f docker-compose.dev.yml up -d
    echo "Waiting for SurrealDB to be ready..."
    sleep 5
fi

# Export env vars for local execution
export SURREALDB_URL=ws://localhost:8000
export SURREALDB_NAMESPACE=elephant
export SURREALDB_DATABASE=test
export SURREALDB_USER=root
export SURREALDB_PASS=root
export SKIP_AUTH=true
export ELEPHANT_HOST=127.0.0.1
export ELEPHANT_PORT=3000
export RUST_LOG=info,elephant=debug

# Pipeline config
export PIPELINE_INGEST_ENABLED=true
export PIPELINE_EMBED_ENABLED=true
export PIPELINE_PROMOTE_ENABLED=true
export PIPELINE_INGEST_POLL_INTERVAL=5
export PIPELINE_EMBED_POLL_INTERVAL=5
export PIPELINE_PROMOTE_POLL_INTERVAL=5
export PIPELINE_EMBED_PROVIDER=openai
export PIPELINE_EMBED_MODEL=text-embedding-3-large
export PIPELINE_EMBED_DIMS=3072
export PIPELINE_AGENTIC_MCP_URL=http://127.0.0.1:3000/mcp/agent
export PIPELINE_AGENTIC_MODEL=claude-sonnet-4-5

# Ensure API keys are set
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "ERROR: ANTHROPIC_API_KEY not set"
    exit 1
fi
if [ -z "${OPENAI_API_KEY:-}" ]; then
    echo "ERROR: OPENAI_API_KEY not set"
    exit 1
fi

echo "========================================="
echo "Elephant Development Mode"
echo "========================================="
echo "SurrealDB: $SURREALDB_URL"
echo "API: http://$ELEPHANT_HOST:$ELEPHANT_PORT"
echo "MCP: $PIPELINE_AGENTIC_MCP_URL"
echo "Model: $PIPELINE_AGENTIC_MODEL"
echo ""

case "$MODE" in
    watch)
        echo "Starting with auto-rebuild (cargo watch)..."
        echo "Changes to source files will trigger automatic rebuild"
        echo ""
        # Install cargo-watch if needed
        if ! command -v cargo-watch &> /dev/null; then
            echo "Installing cargo-watch..."
            cargo install cargo-watch
        fi

        # Run supervisor with watch
        cargo watch -x 'run --release -p elephant-supervisor --features elephant-api/meerkat-extraction,elephant-pipeline/meerkat-extraction'
        ;;

    run)
        echo "Starting without auto-rebuild..."
        cargo run --release -p elephant-supervisor --features "elephant-api/meerkat-extraction,elephant-pipeline/meerkat-extraction"
        ;;

    test)
        echo "Running tests..."
        cargo test --workspace
        ;;

    *)
        echo "Usage: $0 [watch|run|test]"
        echo "  watch - Auto-rebuild on file changes (default)"
        echo "  run   - Run once without watching"
        echo "  test  - Run test suite"
        exit 1
        ;;
esac
