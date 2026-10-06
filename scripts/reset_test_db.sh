#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: scripts/reset_test_db.sh [--ensure-services]

Resets the SurrealDB database used by local Elephant dev/test workflows:
- Stops any running Elephant services (API/Pipeline) to ensure clean state
- Resets the database (REMOVE/DEFINE DATABASE)
- Runs migrations
- Optionally restarts services (if --ensure-services is set)

This script should be run inside a lock (e.g. via scenario_lock in Python) to prevent
parallel tests from killing each other's services.
EOF
}

ensure_services=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ensure-services)
      ensure_services=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_cargo="${repo_root}/scripts/repo-cargo"

case "${ELEPHANT_DB:-surreal-db}" in
  surreal-db|elephant-db)
    ;;
  *)
    echo "Invalid ELEPHANT_DB: ${ELEPHANT_DB}. Expected surreal-db or elephant-db." >&2
    exit 2
    ;;
esac

repo_cargo_env() {
  local key="$1"
  "$repo_cargo" --print-env | awk -F= -v wanted="$key" '$1 == wanted { print substr($0, index($0, "=") + 1) }'
}

cargo_target_dir="${CARGO_TARGET_DIR:-$(repo_cargo_env CARGO_TARGET_DIR)}"
debug_bin_dir="${cargo_target_dir}/debug"

# Namespace and Database
ns="${SURREALDB_NAMESPACE:-elephant}"
db="${SURREALDB_DATABASE:-test}"
surrealdb_url="http://127.0.0.1:8000"
surrealdb_health_url="${surrealdb_url}/health"
surrealdb_ws_url="${SURREALDB_URL:-ws://127.0.0.1:8000}"
surrealdb_user="${SURREALDB_USER:-root}"
surrealdb_pass="${SURREALDB_PASS:-root}"

logs_dir="${repo_root}/.rct/outputs"
mkdir -p "$logs_dir"
api_log="${logs_dir}/elephant-serve.log"
pipeline_log="${logs_dir}/elephant-worker.log"

curl_status() {
  local url="$1"
  curl -s -o /dev/null -w '%{http_code}' "$url" || true
}

wait_for_http_200() {
  local url="$1"
  local attempts="$2"
  local label="$3"

  for _ in $(seq 1 "$attempts"); do
    if [ "$(curl_status "$url")" = "200" ]; then
      return 0
    fi
    sleep 1
  done

  echo "${label} did not become healthy (expected 200 from ${url})." >&2
  return 1
}

kill_process_by_name() {
  local name="$1"
  if command -v pkill >/dev/null 2>&1; then
    pkill -9 -f "${debug_bin_dir}/$name" >/dev/null 2>&1 || true
    pkill -9 -x "$name" >/dev/null 2>&1 || true
  fi
}

# 1. ALWAYS stop services first to ensure a clean slate and release ports.
echo "Stopping existing services..." >&2
kill_process_by_name "elephant"
sleep 1 # Give OS time to release ports

# 2. Ensure SurrealDB is running
db_health="$(curl_status "$surrealdb_health_url")"
if [ "$db_health" != "200" ]; then
  if command -v docker >/dev/null 2>&1 && [ -f docker-compose.yml ]; then
    docker compose up -d surrealdb > /dev/null
    wait_for_http_200 "$surrealdb_health_url" 60 "SurrealDB"
  else
    echo "SurrealDB is not healthy at ${surrealdb_health_url}." >&2
    exit 1
  fi
fi

# 3. Reset Database (SQL)
echo "Resetting database ${ns}/${db}..." >&2
reset_sql="REMOVE DATABASE ${db}; DEFINE DATABASE ${db};"
max_retries=5
for i in $(seq 1 "$max_retries"); do
  reset_result=""
  if ! reset_result="$(curl -sS -u "${surrealdb_user}:${surrealdb_pass}" \
    -H "Surreal-NS: ${ns}" \
    -H "Accept: application/json" \
    -H "Content-Type: text/plain" \
    --data-binary "$reset_sql" \
    "${surrealdb_url}/sql")"; then
    sleep 1
    continue
  fi
  break
done

# 4. Run Migrations
echo "Running migrations..." >&2
SURREALDB_URL="$surrealdb_ws_url" \
  SURREALDB_USER="$surrealdb_user" \
  SURREALDB_PASS="$surrealdb_pass" \
  SURREALDB_DATABASE="$db" \
  SURREALDB_NAMESPACE="$ns" \
  "$repo_cargo" run -p meerkat-recall-bin -- migrate > /dev/null

# 5. Start Services (if requested)
if [ "$ensure_services" -eq 1 ]; then
  echo "Starting elephant serve..." >&2
  # Build first to avoid build output in logs
  "$repo_cargo" build -p meerkat-recall-bin > /dev/null

  RUST_LOG="${RUST_LOG:-info}" \
    PIPELINE_DISABLED=true \
    SURREALDB_URL="$surrealdb_ws_url" \
    SURREALDB_USER="$surrealdb_user" \
    SURREALDB_PASS="$surrealdb_pass" \
    SURREALDB_DATABASE="$db" \
    SURREALDB_NAMESPACE="$ns" \
    nohup "${debug_bin_dir}/meerkat-recall" serve --server-only >> "$api_log" 2>&1 &

  if ! wait_for_http_200 "http://127.0.0.1:3000/health" 30 "elephant serve"; then
    echo "elephant serve failed to start. Logs:" >&2
    tail -n 20 "$api_log" >&2
    exit 1
  fi

  echo "Starting elephant worker..." >&2

  RUST_LOG="${RUST_LOG:-info}" \
    SURREALDB_URL="$surrealdb_ws_url" \
    SURREALDB_USER="$surrealdb_user" \
    SURREALDB_PASS="$surrealdb_pass" \
    SURREALDB_DATABASE="$db" \
    SURREALDB_NAMESPACE="$ns" \
    nohup "${debug_bin_dir}/meerkat-recall" worker >> "$pipeline_log" 2>&1 &
fi

echo "Database reset complete." >&2
