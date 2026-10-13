#!/usr/bin/env bash
# End-to-end test suite for Elephant API
# Run from repository root: ./tests/e2e/run_e2e.sh

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

API_URL="${API_URL:-http://localhost:3000}"
TIMEOUT="${TIMEOUT:-120}"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_test() {
    echo -e "${GREEN}[TEST]${NC} $1"
}

# Check if docker compose is available
check_docker() {
    if command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE="docker-compose"
    elif docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        log_error "Docker Compose not found. Please install Docker Compose."
        exit 1
    fi
    log_info "Using: $DOCKER_COMPOSE"
}

# Start services
start_services() {
    log_info "Starting Docker Compose services..."
    $DOCKER_COMPOSE up -d --build

    log_info "Waiting for services to be healthy..."
    local count=0
    while ! curl -sf "$API_URL/v1/health" > /dev/null 2>&1; do
        count=$((count + 1))
        if [ $count -ge $TIMEOUT ]; then
            log_error "Timeout waiting for API to be ready"
            $DOCKER_COMPOSE logs
            exit 1
        fi
        sleep 1
        echo -n "."
    done
    echo ""
    log_info "Services are ready!"
}

# Stop services
stop_services() {
    log_info "Stopping Docker Compose services..."
    $DOCKER_COMPOSE down -v
}

# Cleanup on exit
cleanup() {
    if [ "${SKIP_CLEANUP:-false}" != "true" ]; then
        stop_services
    fi
}
trap cleanup EXIT

# Test functions
test_health() {
    log_test "Testing health endpoint..."
    local response
    response=$(curl -sf "$API_URL/v1/health")

    if echo "$response" | grep -q '"status":"healthy"'; then
        log_info "Health check passed"
        return 0
    else
        log_error "Health check failed: $response"
        return 1
    fi
}

test_create_entity() {
    log_test "Testing entity creation..."
    local response
    response=$(curl -sf -X POST "$API_URL/v1/entities" \
        -H "Content-Type: application/json" \
        -d '{
            "entity_type": "person",
            "display_name": "Test Person",
            "space_id": "test_space",
            "created_by": "e2e_test"
        }')

    if echo "$response" | grep -q '"entity_type":"person"'; then
        log_info "Entity creation passed"
        # Extract and return the entity ID
        echo "$response" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4
        return 0
    else
        log_error "Entity creation failed: $response"
        return 1
    fi
}

test_get_entity() {
    local entity_id="$1"
    log_test "Testing entity retrieval (id: $entity_id)..."
    local response
    response=$(curl -sf "$API_URL/v1/entities/$entity_id")

    if echo "$response" | grep -q '"display_name":"Test Person"'; then
        log_info "Entity retrieval passed"
        return 0
    else
        log_error "Entity retrieval failed: $response"
        return 1
    fi
}

test_search_entities() {
    log_test "Testing entity search..."
    local response
    response=$(curl -sf -X POST "$API_URL/v1/entities/search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": "Test",
            "limit": 10
        }')

    if echo "$response" | grep -q '"items"'; then
        log_info "Entity search passed"
        return 0
    else
        log_error "Entity search failed: $response"
        return 1
    fi
}

test_create_relationship() {
    local entity1_id="$1"
    local entity2_id="$2"
    log_test "Testing relationship creation..."
    local response
    response=$(curl -sf -X POST "$API_URL/v1/relationships" \
        -H "Content-Type: application/json" \
        -d "{
            \"from_id\": \"$entity1_id\",
            \"to_id\": \"$entity2_id\",
            \"rel_type\": \"knows\",
            \"space_id\": \"test_space\",
            \"created_by\": \"e2e_test\"
        }")

    if echo "$response" | grep -q '"rel_type":"knows"'; then
        log_info "Relationship creation passed"
        return 0
    else
        log_error "Relationship creation failed: $response"
        return 1
    fi
}

test_create_timeline_event() {
    local entity_id="$1"
    log_test "Testing timeline event creation..."
    local response
    response=$(curl -sf -X POST "$API_URL/v1/timeline" \
        -H "Content-Type: application/json" \
        -d "{
            \"entity\": \"$entity_id\",
            \"event_type\": \"meeting\",
            \"title\": \"E2E Test Meeting\",
            \"time\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
            \"space_id\": \"test_space\",
            \"created_by\": \"e2e_test\"
        }")

    if echo "$response" | grep -q '"event_type":"meeting"'; then
        log_info "Timeline event creation passed"
        return 0
    else
        log_error "Timeline event creation failed: $response"
        return 1
    fi
}

test_doc_ingest() {
    log_test "Testing document ingestion..."
    local response
    response=$(curl -sf -X POST "$API_URL/v1/docs/ingest" \
        -H "Content-Type: application/json" \
        -d '{
            "title": "E2E Test Document",
            "text": "This is a test document for the end-to-end test suite. It contains some sample text for chunking and embedding.",
            "collection": "e2e_test",
            "space_id": "test_space",
            "created_by": "e2e_test"
        }')

    if echo "$response" | grep -q '"title":"E2E Test Document"' || echo "$response" | grep -q '"chunks_created"'; then
        log_info "Document ingestion passed"
        return 0
    else
        log_error "Document ingestion failed: $response"
        return 1
    fi
}

test_delete_entity() {
    local entity_id="$1"
    log_test "Testing entity deletion (id: $entity_id)..."
    local response
    local status_code
    status_code=$(curl -sf -o /dev/null -w "%{http_code}" -X DELETE "$API_URL/v1/entities/$entity_id")

    if [ "$status_code" = "204" ] || [ "$status_code" = "200" ]; then
        log_info "Entity deletion passed"
        return 0
    else
        log_error "Entity deletion failed: HTTP $status_code"
        return 1
    fi
}

# Main test runner
run_tests() {
    local passed=0
    local failed=0
    local entity1_id
    local entity2_id

    log_info "Starting E2E test suite..."
    echo "========================================"

    # Health check
    if test_health; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    # Create first entity
    entity1_id=$(test_create_entity)
    if [ -n "$entity1_id" ]; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    # Create second entity
    entity2_id=$(test_create_entity)
    if [ -n "$entity2_id" ]; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    # Get entity
    if [ -n "$entity1_id" ]; then
        if test_get_entity "$entity1_id"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    fi

    # Search entities
    if test_search_entities; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    # Create relationship
    if [ -n "$entity1_id" ] && [ -n "$entity2_id" ]; then
        if test_create_relationship "$entity1_id" "$entity2_id"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    fi

    # Create timeline event
    if [ -n "$entity1_id" ]; then
        if test_create_timeline_event "$entity1_id"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    fi

    # Document ingestion
    if test_doc_ingest; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    # Delete entity
    if [ -n "$entity1_id" ]; then
        if test_delete_entity "$entity1_id"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    fi

    echo "========================================"
    log_info "E2E Test Results: $passed passed, $failed failed"

    if [ $failed -gt 0 ]; then
        log_error "Some tests failed!"
        return 1
    else
        log_info "All tests passed!"
        return 0
    fi
}

# Main execution
main() {
    log_info "Elephant E2E Test Suite"
    echo "========================================"

    check_docker
    start_services
    run_tests
}

main "$@"
