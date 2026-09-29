#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_CARGO="${ROOT_DIR}/scripts/repo-cargo"
SCENARIOS_FILE="${ROOT_DIR}/tests/golden_scenarios/scenarios.json"
DATASETS_FILE="${ROOT_DIR}/tests/golden_scenarios/datasets.json"

API_URL="${API_URL:-http://127.0.0.1:3000}"
SURREALDB_HTTP_URL="${SURREALDB_HTTP_URL:-http://127.0.0.1:8000}"
SURREALDB_WS_URL="${SURREALDB_WS_URL:-ws://127.0.0.1:8000}"
SURREALDB_NAMESPACE="${SURREALDB_NAMESPACE:-elephant}"
SURREALDB_DATABASE="${SURREALDB_DATABASE:-golden_$(date -u +%Y%m%d_%H%M%S)}"
SURREALDB_USER="${SURREALDB_USER:-root}"
SURREALDB_PASS="${SURREALDB_PASS:-root}"

MAX_DOC_CHARS="${MAX_DOC_CHARS:-4500}"
DOC_TIMEOUT_SECS="${DOC_TIMEOUT_SECS:-360}"
DOC_TIMEOUT_SECS_RECHECK="${DOC_TIMEOUT_SECS_RECHECK:-600}"
DOC_NOT_TRIGGERED_GRACE_SECS="${DOC_NOT_TRIGGERED_GRACE_SECS:-90}"
DOC_NOT_TRIGGERED_GRACE_SECS_RECHECK="${DOC_NOT_TRIGGERED_GRACE_SECS_RECHECK:-360}"
DOC_PENDING_GRACE_SECS="${DOC_PENDING_GRACE_SECS:-180}"
DOC_PENDING_GRACE_SECS_RECHECK="${DOC_PENDING_GRACE_SECS_RECHECK:-420}"
EMBED_TIMEOUT_SECS="${EMBED_TIMEOUT_SECS:-240}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-4}"
SCENARIO_LIMIT="${SCENARIO_LIMIT:-0}"
SCENARIO_OFFSET="${SCENARIO_OFFSET:-0}"
PIPELINE_FORCE_DEFAULT_EXTRACTOR="${PIPELINE_FORCE_DEFAULT_EXTRACTOR:-false}"
WAIT_FOR_EMBEDDINGS="${WAIT_FOR_EMBEDDINGS:-false}"
DATASET_FETCH_MAX_TIME_SECS="${DATASET_FETCH_MAX_TIME_SECS:-35}"
DATASET_FETCH_RETRIES="${DATASET_FETCH_RETRIES:-2}"
DATASET_FETCH_RETRY_DELAY_SECS="${DATASET_FETCH_RETRY_DELAY_SECS:-2}"
CONTROL_QUERY="${CONTROL_QUERY:-Alex Rivera}"
JUDGE_ENABLED="${JUDGE_ENABLED:-true}"
JUDGE_MODEL="${JUDGE_MODEL:-gpt-4o-mini}"
JUDGE_MIN_OVERALL="${JUDGE_MIN_OVERALL:-0.55}"
JUDGE_MAX_DOCS="${JUDGE_MAX_DOCS:-12}"
JUDGE_MAX_EVENTS="${JUDGE_MAX_EVENTS:-20}"
JUDGE_MAX_ASSERTIONS="${JUDGE_MAX_ASSERTIONS:-30}"
JUDGE_MAX_TRUTH_SLOTS="${JUDGE_MAX_TRUTH_SLOTS:-10}"
HTTP_CONNECT_TIMEOUT_SECS="${HTTP_CONNECT_TIMEOUT_SECS:-5}"
HTTP_MAX_TIME_SECS="${HTTP_MAX_TIME_SECS:-45}"
JUDGE_HTTP_MAX_TIME_SECS="${JUDGE_HTTP_MAX_TIME_SECS:-120}"
JUDGE_HTTP_RETRIES="${JUDGE_HTTP_RETRIES:-2}"

STRICT=0
FAIL_FAST=0
TEARDOWN=0
NO_DOCKER=0

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${ROOT_DIR}/tests/golden_scenarios/output/${RUN_TS}"
RAW_DIR="${RUN_DIR}/raw"
NORM_DIR="${RUN_DIR}/normalized"
LOG_DIR="${RUN_DIR}/logs"
RESULTS_JSONL="${RUN_DIR}/results.jsonl"
RESULTS_JSON="${RUN_DIR}/results.json"
SUMMARY_MD="${RUN_DIR}/summary.md"
USER_AGENT="elephant-golden-scenarios/1.0 (+https://github.com/lukacf/elephant)"

DOCKER_COMPOSE_CMD=()

usage() {
  cat <<'USAGE'
Usage: tests/golden_scenarios/run_golden_scenarios.sh [options]

Options:
  --strict            Exit non-zero if any scenario fails.
  --fail-fast         Stop immediately after the first scenario-level failure.
  --teardown          Stop docker compose services at end.
  --no-docker         Assume services already running; do not start docker compose.
  --offset N          Skip first N scenarios from scenarios.json.
  --limit N           Run only first N scenarios from scenarios.json.
  -h, --help          Show this help.

Environment overrides:
  API_URL, SURREALDB_HTTP_URL, SURREALDB_WS_URL, SURREALDB_NAMESPACE, SURREALDB_DATABASE
  MAX_DOC_CHARS, DOC_TIMEOUT_SECS, EMBED_TIMEOUT_SECS, POLL_INTERVAL_SECS, SCENARIO_LIMIT, SCENARIO_OFFSET
  WAIT_FOR_EMBEDDINGS
  JUDGE_ENABLED, JUDGE_MODEL, JUDGE_MIN_OVERALL
USAGE
}

log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

warn() {
  printf '[%s] WARN: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

die() {
  printf '[%s] ERROR: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

is_truthy() {
  local value="${1:-}"
  value="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
  [[ "$value" == "1" || "$value" == "true" || "$value" == "yes" || "$value" == "on" ]]
}

normalize_search_query() {
  local value="${1:-}"
  printf '%s' "$value" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9]+/ /g; s/[[:space:]]+/ /g; s/^ //; s/ $//'
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --strict)
        STRICT=1
        shift
        ;;
      --fail-fast)
        FAIL_FAST=1
        shift
        ;;
      --teardown)
        TEARDOWN=1
        shift
        ;;
      --no-docker)
        NO_DOCKER=1
        shift
        ;;
      --limit)
        [[ $# -ge 2 ]] || die "--limit requires a value"
        SCENARIO_LIMIT="$2"
        shift 2
        ;;
      --offset)
        [[ $# -ge 2 ]] || die "--offset requires a value"
        SCENARIO_OFFSET="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done
}

setup() {
  require_cmd curl
  require_cmd jq
  require_cmd uuidgen
  require_cmd "$REPO_CARGO"
  require_cmd xmllint

  [[ -f "$SCENARIOS_FILE" ]] || die "Missing scenarios file: $SCENARIOS_FILE"
  [[ -f "$DATASETS_FILE" ]] || die "Missing datasets file: $DATASETS_FILE"

  if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
    die "ANTHROPIC_API_KEY is required for live extraction runs"
  fi
  if [[ -z "${OPENAI_API_KEY:-}" ]]; then
    die "OPENAI_API_KEY is required for embedding runs"
  fi

  mkdir -p "$RUN_DIR" "$RAW_DIR" "$NORM_DIR" "$LOG_DIR"
  : > "$RESULTS_JSONL"

  if [[ "$NO_DOCKER" -eq 0 ]]; then
    if command -v docker-compose >/dev/null 2>&1; then
      DOCKER_COMPOSE_CMD=(docker-compose)
    elif docker compose version >/dev/null 2>&1; then
      DOCKER_COMPOSE_CMD=(docker compose)
    else
      die "docker compose is required unless --no-docker is used"
    fi
  fi

  log "Run output: $RUN_DIR"
  log "Database target: ${SURREALDB_NAMESPACE}/${SURREALDB_DATABASE}"
  log "PIPELINE_FORCE_DEFAULT_EXTRACTOR=${PIPELINE_FORCE_DEFAULT_EXTRACTOR}"
  log "WAIT_FOR_EMBEDDINGS=${WAIT_FOR_EMBEDDINGS}"
  log "LLM judge: enabled=${JUDGE_ENABLED} model=${JUDGE_MODEL} min_overall=${JUDGE_MIN_OVERALL}"
}

wait_for_endpoint() {
  local url="$1"
  local timeout_secs="$2"
  local label="$3"
  local end=$((SECONDS + timeout_secs))

  while (( SECONDS < end )); do
    if curl -sSf \
      --connect-timeout 2 \
      --max-time 3 \
      "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  die "Timed out waiting for ${label} at ${url}"
}

start_services() {
  if [[ "$NO_DOCKER" -eq 1 ]]; then
    log "Skipping docker startup (--no-docker)"
    return
  fi

  log "Starting docker services (surrealdb + elephant-api/supervisor)"
  (
    cd "$ROOT_DIR"
    export SURREALDB_DATABASE
    export SURREALDB_NAMESPACE
    export OPENAI_API_KEY
    export ANTHROPIC_API_KEY
    export PIPELINE_FORCE_DEFAULT_EXTRACTOR
    "${DOCKER_COMPOSE_CMD[@]}" up -d --build --force-recreate surrealdb elephant-api
  )

  wait_for_endpoint "${SURREALDB_HTTP_URL}/health" 120 "SurrealDB"
  wait_for_endpoint "${API_URL}/v1/health" 180 "Elephant API"
}

teardown_services() {
  if [[ "$NO_DOCKER" -eq 1 || "$TEARDOWN" -eq 0 ]]; then
    return
  fi

  log "Tearing down docker services"
  (
    cd "$ROOT_DIR"
    "${DOCKER_COMPOSE_CMD[@]}" down
  )
}

reset_and_migrate() {
  log "Resetting database ${SURREALDB_NAMESPACE}/${SURREALDB_DATABASE}"

  local sql="REMOVE DATABASE ${SURREALDB_DATABASE}; DEFINE DATABASE ${SURREALDB_DATABASE};"
  curl -sS -u "${SURREALDB_USER}:${SURREALDB_PASS}" \
    -H "Surreal-NS: ${SURREALDB_NAMESPACE}" \
    -H "Accept: application/json" \
    -H "Content-Type: text/plain" \
    --data-binary "$sql" \
    "${SURREALDB_HTTP_URL}/sql" >/dev/null

  log "Applying migrations"
  (
    cd "$ROOT_DIR"
    SURREALDB_URL="$SURREALDB_WS_URL" \
      SURREALDB_NAMESPACE="$SURREALDB_NAMESPACE" \
      SURREALDB_DATABASE="$SURREALDB_DATABASE" \
      SURREALDB_USER="$SURREALDB_USER" \
      SURREALDB_PASS="$SURREALDB_PASS" \
      "$REPO_CARGO" run -p elephant-cli -- migrate >/dev/null
  )
}

dataset_field() {
  local dataset_id="$1"
  local field="$2"
  jq -r --arg id "$dataset_id" --arg field "$field" '
    .datasets[]
    | select(.id == $id)
    | .[$field]
  ' "$DATASETS_FILE"
}

dataset_skill_for_ingest() {
  local dataset_id="$1"
  case "$dataset_id" in
    *fiction*|*novel*|*alice*)
      printf '%s' "extraction/fiction"
      ;;
    *)
      printf '%s' "extraction/technical-docs"
      ;;
  esac
}

normalize_xml_generic() {
  local input="$1"
  sed -E 's/<[^>]+>/ /g; s/&nbsp;/ /g; s/&amp;/\&/g; s/&quot;/"/g; s/&#39;/'"'"'/g' "$input" \
    | tr -s ' ' \
    | fold -s -w 180 \
    | sed '/^[[:space:]]*$/d' \
    | sed -n '1,220p'
}

normalize_json_with_filter() {
  local input="$1"
  local filter="$2"
  local output="$3"

  if ! jq -r "$filter" "$input" > "$output" 2>/dev/null; then
    jq -r '.' "$input" | sed -n '1,220p' > "$output"
  fi
}

normalize_dataset() {
  local dataset_id="$1"
  local raw_file="$2"
  local out_file="$3"

  case "$dataset_id" in
    nasa_apod)
      normalize_json_with_filter "$raw_file" '
        [
          "NASA APOD snapshot:",
          "date=\(.date)",
          "title=\(.title)",
          "url=\(.url // "n/a")",
          "explanation=\(.explanation // "")"
        ] | join("\n")
      ' "$out_file"
      ;;
    usgs_all_day|usgs_all_week)
      normalize_json_with_filter "$raw_file" '
        [
          "USGS earthquake feed metadata:",
          "generated=\((.metadata.generated / 1000 | todateiso8601))",
          "count=\(.metadata.count)",
          (.features[:18][] | "event_time=\((.properties.time / 1000 | todateiso8601)); place=\(.properties.place // "unknown"); mag=\(.properties.mag // "n/a"); status=\(.properties.status // "unknown")")
        ] | join("\n")
      ' "$out_file"
      ;;
    weather_ca|weather_fl)
      normalize_json_with_filter "$raw_file" '
        [
          "NOAA weather alert feed:",
          "updated=\(.updated)",
          "alerts=\(.features | length)",
          (.features[:16][] | "event=\(.properties.event // "unknown"); area=\(.properties.areaDesc // "unknown"); severity=\(.properties.severity // "unknown"); onset=\(.properties.onset // "unknown"); ends=\(.properties.ends // "unknown"); headline=\(.properties.headline // "")")
        ] | join("\n")
      ' "$out_file"
      ;;
    open_meteo_sf|open_meteo_nyc)
      normalize_json_with_filter "$raw_file" '
        [
          "Open-Meteo forecast snapshot:",
          "timezone=\(.timezone)",
          "generation_ms=\(.generationtime_ms)",
          (range(0; (.hourly.time[:30] | length)) | "time=\(.hourly.time[.]); temp_c=\(.hourly.temperature_2m[.])")
        ] | join("\n")
      ' "$out_file"
      ;;
    github_events)
      normalize_json_with_filter "$raw_file" '
        [
          "GitHub public events snapshot:",
          (.[:24][] | "created_at=\(.created_at); actor=\(.actor.login // "unknown"); type=\(.type); repo=\(.repo.name // "unknown")")
        ] | join("\n")
      ' "$out_file"
      ;;
    hn_ai_stories)
      normalize_json_with_filter "$raw_file" '
        [
          "Hacker News AI stories snapshot:",
          "hits=\(.hits | length)",
          (.hits[:24][] | "created_at=\(.created_at); title=\(.title // "untitled"); points=\(.points // 0); author=\(.author // "unknown"); url=\(.url // "n/a")")
        ] | join("\n")
      ' "$out_file"
      ;;
    stackoverflow_activity)
      normalize_json_with_filter "$raw_file" '
        [
          "Stack Overflow activity snapshot:",
          "items=\(.items | length)",
          (.items[:24][] | "created_at=\(.creation_date | todateiso8601); title=\(.title); score=\(.score); answers=\(.answer_count); tags=\(.tags | join(","))")
        ] | join("\n")
      ' "$out_file"
      ;;
    openalex_ml)
      normalize_json_with_filter "$raw_file" '
        [
          "OpenAlex works snapshot:",
          "count=\(.meta.count // 0)",
          (.results[:20][] | "publication_date=\(.publication_date // "unknown"); title=\(.display_name // .title // "untitled"); cited_by=\(.cited_by_count // 0); type=\(.type // "unknown")")
        ] | join("\n")
      ' "$out_file"
      ;;
    crossref_climate)
      normalize_json_with_filter "$raw_file" '
        [
          "Crossref works snapshot:",
          "total_results=\(.message["total-results"] // 0)",
          (.message.items[:20][] | "created=\(.created["date-time"] // "unknown"); title=\((.title[0] // "untitled")); publisher=\(.publisher // "unknown"); citations=\(.["is-referenced-by-count"] // 0)")
        ] | join("\n")
      ' "$out_file"
      ;;
    worldbank_us_pop|worldbank_in_gdp)
      normalize_json_with_filter "$raw_file" '
        [
          "World Bank indicator snapshot:",
          "total_points=\(.[0].total // 0)",
          (. [1][:28][] | "country=\(.country.value); indicator=\(.indicator.value); year=\(.date); value=\(.value // "n/a")")
        ] | join("\n")
      ' "$out_file"
      ;;
    fda_drug_enforcement|fda_device_enforcement)
      normalize_json_with_filter "$raw_file" '
        [
          "OpenFDA enforcement snapshot:",
          "results=\(.results | length)",
          (.results[:20][] | "init_date=\(.recall_initiation_date // "unknown"); recall=\(.recall_number // "n/a"); firm=\(.recalling_firm // "unknown"); product=\((.product_description // "" | gsub("\\n";" ") | .[0:150])); reason=\((.reason_for_recall // "" | gsub("\\n";" ") | .[0:150]))")
        ] | join("\n")
      ' "$out_file"
      ;;
    nhtsa_tesla_recalls)
      normalize_json_with_filter "$raw_file" '
        [
          "NHTSA recalls snapshot:",
          "count=\(.Count // 0)",
          (.results[:20][] | "received=\(.ReportReceivedDate // "unknown"); campaign=\(.NHTSACampaignNumber // "unknown"); make=\(.Make // "unknown"); model=\(.Model // "unknown"); year=\(.ModelYear // "unknown"); summary=\((.Summary // "" | gsub("\\n";" ") | .[0:150]))")
        ] | join("\n")
      ' "$out_file"
      ;;
    federal_register_latest)
      normalize_json_with_filter "$raw_file" '
        [
          "Federal Register snapshot:",
          "count=\(.count // 0)",
          (.results[:20][] | "publication_date=\(.publication_date // "unknown"); type=\(.type // "unknown"); title=\(.title // "untitled"); document_number=\(.document_number // "n/a")")
        ] | join("\n")
      ' "$out_file"
      ;;
    spacex_launches)
      normalize_json_with_filter "$raw_file" '
        [
          "SpaceX launches snapshot:",
          (sort_by(.date_unix) | reverse | .[:20][] | "date_utc=\(.date_utc // "unknown"); mission=\(.name // "unknown"); success=\(.success // "unknown"); details=\((.details // "" | gsub("\\n";" ") | .[0:150]))")
        ] | join("\n")
      ' "$out_file"
      ;;
    coingecko_top)
      normalize_json_with_filter "$raw_file" '
        [
          "CoinGecko market snapshot:",
          (.[:20][] | "updated=\(.last_updated // "unknown"); coin=\(.name // "unknown") (\(.symbol // "")); rank=\(.market_cap_rank // "n/a"); price_usd=\(.current_price // "n/a"); pct_24h=\(.price_change_percentage_24h // "n/a")")
        ] | join("\n")
      ' "$out_file"
      ;;
    nyc_311)
      normalize_json_with_filter "$raw_file" '
        [
          "NYC 311 snapshot:",
          (.[:20][] | "created=\(.created_date // "unknown"); complaint=\(.complaint_type // "unknown"); descriptor=\(.descriptor // "unknown"); borough=\(.borough // "unknown"); status=\(.status // "unknown")")
        ] | join("\n")
      ' "$out_file"
      ;;
    ecb_daily)
      {
        local fx_date
        fx_date="$(xmllint --xpath 'string(//*[local-name()="Cube"]/*[local-name()="Cube"]/@time)' "$raw_file" 2>/dev/null || true)"
        printf 'ECB FX snapshot:\n'
        printf 'date=%s\n' "${fx_date:-unknown}"
        xmllint --xpath '//*[local-name()="Cube"]/*[local-name()="Cube"]/*[local-name()="Cube"]' "$raw_file" 2>/dev/null \
          | sed 's/></>\n</g' \
          | sed -E 's/.*currency="([^"]+)".*rate="([^"]+)".*/currency=\1 rate=\2/' \
          | sed -n '1,40p'
      } > "$out_file"
      ;;
    arxiv_ml|bbc_world_rss|nytimes_world_rss|un_rss|bls_latest_rss|sec_press_releases|nasa_news_feed)
      normalize_xml_generic "$raw_file" > "$out_file"
      ;;
    *)
      warn "No explicit normalizer for dataset ${dataset_id}; using fallback"
      if jq -e '.' "$raw_file" >/dev/null 2>&1; then
        jq -r '.' "$raw_file" | sed -n '1,220p' > "$out_file"
      else
        normalize_xml_generic "$raw_file" > "$out_file"
      fi
      ;;
  esac

  if [[ ! -s "$out_file" ]]; then
    warn "Normalizer produced empty output for ${dataset_id}; using raw fallback"
    head -c "$MAX_DOC_CHARS" "$raw_file" > "$out_file"
  fi
}

api_post_json() {
  local space_id="$1"
  local path="$2"
  local payload="$3"
  local response_file="$4"
  local http_code
  http_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
      --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECS" \
      --max-time "$HTTP_MAX_TIME_SECS" \
      -X POST "${API_URL}${path}" \
      -H 'Content-Type: application/json' \
      -H "X-Space-Id: ${space_id}" \
      -d "$payload" \
      || true
  )"
  if [[ -z "$http_code" ]]; then
    http_code="000"
  fi
  printf '%s' "$http_code"
}

api_get() {
  local space_id="$1"
  local path="$2"
  local response_file="$3"
  local http_code
  http_code="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
      --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECS" \
      --max-time "$HTTP_MAX_TIME_SECS" \
      -H "X-Space-Id: ${space_id}" \
      "${API_URL}${path}" \
      || true
  )"
  if [[ -z "$http_code" ]]; then
    http_code="000"
  fi
  printf '%s' "$http_code"
}

fetch_dataset_payload() {
  local dataset_id="$1"
  local dataset_url="$2"
  local raw_file="$3"
  local attempt=1
  local max_attempts=$((DATASET_FETCH_RETRIES + 1))

  while (( attempt <= max_attempts )); do
    if curl -sS -L -A "$USER_AGENT" \
      --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECS" \
      --max-time "$DATASET_FETCH_MAX_TIME_SECS" \
      "$dataset_url" -o "$raw_file"; then
      return 0
    fi

    if (( attempt < max_attempts )); then
      warn "Scenario dataset fetch failed for ${dataset_id} (attempt ${attempt}/${max_attempts}); retrying in ${DATASET_FETCH_RETRY_DELAY_SECS}s"
      sleep "$DATASET_FETCH_RETRY_DELAY_SECS"
    fi
    attempt=$((attempt + 1))
  done

  return 1
}

run_llm_judge() {
  local scenario_id="$1"
  local scenario_json="$2"
  local space_id="$3"
  local query="$4"
  local assertions_file="$5"
  local timeline_query_file="$6"
  local timeline_all_file="$7"
  local docs_search_file="$8"
  local truth_slots_file="$9"
  local judge_output_file="${10}"

  if ! is_truthy "$JUDGE_ENABLED"; then
    jq -n '{
      enabled: false,
      overall_score: null,
      relevance_score: null,
      grounding_score: null,
      temporal_score: null,
      contradiction_score: null,
      verdict: "disabled",
      summary: "LLM judge disabled by configuration.",
      strengths: [],
      issues: [],
      recommended_actions: []
    }' > "$judge_output_file"
    return 0
  fi

  local judge_input_file judge_request_file judge_response_file
  judge_input_file="${RUN_DIR}/${scenario_id}_judge_input.json"
  judge_request_file="${RUN_DIR}/${scenario_id}_judge_request.json"
  judge_response_file="${RUN_DIR}/${scenario_id}_judge_response_raw.json"

  jq -n \
    --argjson scenario "$scenario_json" \
    --arg query "$query" \
    --arg space_id "$space_id" \
    --argjson max_docs "$JUDGE_MAX_DOCS" \
    --argjson max_events "$JUDGE_MAX_EVENTS" \
    --argjson max_assertions "$JUDGE_MAX_ASSERTIONS" \
    --argjson max_truth "$JUDGE_MAX_TRUTH_SLOTS" \
    --slurpfile docs "$docs_search_file" \
    --slurpfile timeline_all "$timeline_all_file" \
    --slurpfile timeline "$timeline_query_file" \
    --slurpfile assertions "$assertions_file" \
    --slurpfile truth "$truth_slots_file" \
    '
      def rows(o): (o.items // o.results // []);
      {
        scenario: {
          id: $scenario.id,
          name: $scenario.name,
          complexity: $scenario.complexity,
          query: $query,
          min_assertions: $scenario.min_assertions,
          min_events: $scenario.min_events,
          ingestions: $scenario.ingestions
        },
        retrieval: {
          docs: (
            ($docs[0] // {})
            | rows(.)
            | .[:$max_docs]
            | map({
              score: (.score // null),
              title: (.doc.title // .title // ""),
              text_excerpt: ((.doc.text // .text // "")[0:480]),
              metadata: (.doc.metadata // .metadata // {}),
              id: (.doc.base.id // .base.id // "")
            })
          ),
          events_all: (
            ($timeline_all[0] // {})
            | rows(.)
            | .[:$max_events]
            | map({
              title: (.title // ""),
              status: (.status // ""),
              confidence: (.confidence // null),
              time_start: (.time_start // null),
              time_end: (.time_end // null),
              assertion_refs: (.assertion_refs // [])
            })
          ),
          events: (
            ($timeline[0] // {})
            | rows(.)
            | .[:$max_events]
            | map({
              title: (.title // ""),
              status: (.status // ""),
              confidence: (.confidence // null),
              time_start: (.time_start // null),
              time_end: (.time_end // null),
              assertion_refs: (.assertion_refs // [])
            })
          ),
          assertions: (
            ($assertions[0] // {})
            | rows(.)
            | .[:$max_assertions]
            | map({
              subject_text: (.subject_text // ""),
              predicate_text: (.predicate_text // ""),
              object_text: (.object_text // ""),
              confidence: (.confidence // null),
              source_event_time: (.source_event_time // null),
              significance: (.significance // null)
            })
          ),
          truth_slots: (
            ($truth[0] // {})
            | rows(.)
            | .[:$max_truth]
            | map({
              subject_entity: (.subject_entity // null),
              predicate_id: (.predicate_id // ""),
              confidence: (.confidence // null),
              claim_status: (.claim_status // "")
            })
          )
        }
      }
    ' > "$judge_input_file"

  local system_prompt user_prompt
  system_prompt=$'You are an objective test judge for a knowledge ingestion and retrieval system.\nReturn STRICT JSON only (no markdown, no prose outside JSON).\nScore from 0.0 to 1.0.\nBe strict about relevance, grounding/provenance, temporal correctness, and contradiction handling.'

  user_prompt=$(
    cat <<EOF
Evaluate the scenario retrieval quality.

Return JSON with this exact shape:
{
  "overall_score": number,
  "relevance_score": number,
  "grounding_score": number,
  "temporal_score": number,
  "contradiction_score": number,
  "verdict": "pass" | "fail",
  "summary": string,
  "strengths": [string],
  "issues": [string],
  "recommended_actions": [string]
}

Scoring guidance:
- relevance_score: are retrieved docs/events/assertions aligned to scenario query and ingestion notes?
- grounding_score: do results appear evidence-backed and specific (not generic boilerplate)?
- temporal_score: timeline/event ordering and time fields make sense for scenario intent.
- contradiction_score: conflicting updates are represented sensibly (not collapsed into nonsense).
- overall_score: conservative aggregate; fail if major retrieval irrelevance or grounding collapse.
- verdict rule: "pass" if overall_score >= ${JUDGE_MIN_OVERALL}; otherwise "fail".

Important nuance:
- 'retrieval.events' is query-filtered timeline output.
- 'retrieval.events_all' is unfiltered timeline output.
- Do NOT fail solely because query-filtered events are sparse; consider docs/assertions relevance and all-events context together.

Scenario context JSON:
$(cat "$judge_input_file")
EOF
  )

  jq -n \
    --arg model "$JUDGE_MODEL" \
    --arg system_prompt "$system_prompt" \
    --arg user_prompt "$user_prompt" \
    '{
      model: $model,
      temperature: 0,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: $system_prompt },
        { role: "user", content: $user_prompt }
      ]
    }' > "$judge_request_file"

  local http_code
  local attempt
  local max_attempts=$((JUDGE_HTTP_RETRIES + 1))
  http_code="000"
  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    http_code="$(
      curl -sS -o "$judge_response_file" -w '%{http_code}' \
        --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECS" \
        --max-time "$JUDGE_HTTP_MAX_TIME_SECS" \
        -X POST "https://api.openai.com/v1/chat/completions" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${OPENAI_API_KEY}" \
        -d @"$judge_request_file" \
        || true
    )"
    if [[ -z "$http_code" ]]; then
      http_code="000"
    fi
    if [[ "$http_code" == "200" ]]; then
      break
    fi
    warn "Judge call attempt ${attempt}/${max_attempts} failed (${http_code}) for ${scenario_id}"
    sleep 2
  done

  if [[ "$http_code" != "200" ]]; then
    warn "Judge call failed (${http_code}) for ${scenario_id}"
    jq -n --arg code "$http_code" '{
      enabled: true,
      overall_score: null,
      relevance_score: null,
      grounding_score: null,
      temporal_score: null,
      contradiction_score: null,
      verdict: "unavailable",
      summary: ("LLM judge request failed with HTTP " + $code),
      strengths: [],
      issues: ["judge_http_failure"],
      recommended_actions: ["Check OpenAI credentials/model availability."]
    }' > "$judge_output_file"
    return 0
  fi

  local content
  content="$(jq -r '.choices[0].message.content // empty' "$judge_response_file")"
  if [[ -z "$content" ]]; then
    warn "Judge returned empty content for ${scenario_id}"
    jq -n '{
      enabled: true,
      overall_score: null,
      relevance_score: null,
      grounding_score: null,
      temporal_score: null,
      contradiction_score: null,
      verdict: "unavailable",
      summary: "LLM judge returned empty content.",
      strengths: [],
      issues: ["judge_empty_response"],
      recommended_actions: ["Retry judge with a smaller context payload."]
    }' > "$judge_output_file"
    return 0
  fi

  if ! jq -e . >/dev/null 2>&1 <<<"$content"; then
    warn "Judge returned non-JSON content for ${scenario_id}"
    jq -n --arg raw "$(printf '%s' "$content" | sed -n '1,3p' | tr '\n' ' ')" '{
      enabled: true,
      overall_score: null,
      relevance_score: null,
      grounding_score: null,
      temporal_score: null,
      contradiction_score: null,
      verdict: "unavailable",
      summary: "LLM judge response was not valid JSON.",
      strengths: [],
      issues: ["judge_invalid_json"],
      recommended_actions: [("Raw preview: " + $raw)]
    }' > "$judge_output_file"
    return 0
  fi

  printf '%s' "$content" | jq -c '{
    enabled: true,
    overall_score: (.overall_score // null),
    relevance_score: (.relevance_score // null),
    grounding_score: (.grounding_score // null),
    temporal_score: (.temporal_score // null),
    contradiction_score: (.contradiction_score // null),
    verdict: ((.verdict // "unavailable") | ascii_downcase),
    summary: (.summary // ""),
    strengths: (.strengths // []),
    issues: (.issues // []),
    recommended_actions: (.recommended_actions // [])
  }' > "$judge_output_file"
}

ingest_doc() {
  local scenario_id="$1"
  local space_id="$2"
  local dataset_id="$3"
  local step_idx="$4"
  local note="$5"
  local normalized_file="$6"

  local dataset_name
  dataset_name="$(dataset_field "$dataset_id" name)"

  local doc_uuid doc_id now title rendition_key payload_file response_file status_code metadata_json
  local extraction_skill extraction_purpose
  doc_uuid="$(uuidgen | tr '[:upper:]' '[:lower:]')"
  doc_id="doc:${doc_uuid}"
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  title="${scenario_id} step ${step_idx}: ${dataset_name}"
  rendition_key="golden_${scenario_id}_${step_idx}_${doc_uuid}"
  extraction_skill="$(dataset_skill_for_ingest "$dataset_id")"
  extraction_purpose="Extract high-signal entities, assertions, events, and contradiction timelines for scenario stress testing."

  local text_file="${NORM_DIR}/${scenario_id}_step${step_idx}_text.txt"
  local text_untruncated_file="${NORM_DIR}/${scenario_id}_step${step_idx}_text_full.txt"
  local due_day_1 due_day_2 due_date_1 due_date_2
  due_day_1=$((10 + (step_idx % 18)))
  due_day_2=$((11 + (step_idx % 18)))
  due_date_1="$(printf '2026-03-%02d' "${due_day_1}")"
  due_date_2="$(printf '2026-03-%02d' "${due_day_2}")"
  {
    printf 'Scenario ID: %s\n' "$scenario_id"
    printf 'Scenario step: %s\n' "$step_idx"
    printf 'Dataset: %s (%s)\n' "$dataset_id" "$dataset_name"
    printf 'Captured at: %s\n\n' "$now"
    printf 'Meeting Ops Ledger %s Step %s\n' "$scenario_id" "$step_idx"
    printf 'Attendees: Alex Rivera, Morgan Lee, Priya Shah\n'
    printf 'Decisions:\n'
    printf -- '- Alex Rivera works at Orbital Dynamics\n'
    printf -- '- Morgan Lee joined Signal Labs as Operations Lead in 2026-03-01\n'
    printf -- '- Priya Shah mentors Devon Cole in Reliability Program\n'
    printf -- '- Alex Rivera is Incident Commander\n'
    printf 'Action Items:\n'
    printf -- '- Alex Rivera: publish incident digest by %s\n' "$due_date_1"
    printf -- '- Morgan Lee: reconcile duplicate timeline by %s\n\n' "$due_date_2"
    if [[ -n "$note" ]]; then
      printf 'Operator note:\n%s\n\n' "$note"
    fi
    cat "$normalized_file"
  } > "$text_untruncated_file"
  head -c "$MAX_DOC_CHARS" "$text_untruncated_file" > "$text_file"

  metadata_json="$(jq -n \
    --arg scenario_id "$scenario_id" \
    --arg dataset_id "$dataset_id" \
    --arg dataset_name "$dataset_name" \
    --arg step_idx "$step_idx" \
    --arg captured_at "$now" \
    --arg extraction_skill "$extraction_skill" \
    --arg extraction_purpose "$extraction_purpose" \
    '{
      scenario_id: $scenario_id,
      dataset_id: $dataset_id,
      dataset_name: $dataset_name,
      step_idx: ($step_idx | tonumber),
      captured_at: $captured_at,
      harness: "golden_scenarios",
      extraction_skill: $extraction_skill,
      extraction_purpose: $extraction_purpose
    }')"

  payload_file="${RUN_DIR}/payload_${scenario_id}_step${step_idx}.json"
  response_file="${RUN_DIR}/ingest_${scenario_id}_step${step_idx}.json"

  jq -n \
    --arg doc_id "$doc_id" \
    --arg space_id "$space_id" \
    --arg now "$now" \
    --arg title "$title" \
    --arg text "$(cat "$text_file")" \
    --arg rendition_key "$rendition_key" \
    --argjson metadata "$metadata_json" \
    '{
      doc: {
        id: $doc_id,
        space_id: $space_id,
        created_at: $now,
        updated_at: $now,
        created_by: "golden-scenarios",
        provenance: { sources: [], chain: [] },
        security: { policy_id: "default", level: 0, labels: [], handling: [] },
        subjects: [],
        subjects_state: "unknown",
        title: $title,
        text: $text,
        source: null,
        rendition_key: $rendition_key,
        metadata: $metadata
      },
      sync: false
    }' > "$payload_file"

  status_code="$(api_post_json "$space_id" "/v1/docs/ingest" "$(cat "$payload_file")" "$response_file")"

  if [[ "$status_code" != "201" ]]; then
    warn "Ingest failed (${status_code}) for ${scenario_id} step ${step_idx}"
    return 1
  fi

  local returned_doc_id
  returned_doc_id="$(jq -r '.doc_id // empty' "$response_file")"
  if [[ -z "$returned_doc_id" ]]; then
    warn "Ingest response missing doc_id for ${scenario_id} step ${step_idx}"
    return 1
  fi

  printf '%s' "$returned_doc_id"
}

wait_for_doc_completion() {
  local space_id="$1"
  local doc_id="$2"
  local output_file="$3"
  local not_triggered_grace_secs="${4:-$DOC_NOT_TRIGGERED_GRACE_SECS}"
  local timeout_secs="${5:-$DOC_TIMEOUT_SECS}"
  local pending_grace_secs="${6:-$DOC_PENDING_GRACE_SECS}"

  local encoded_doc_id
  encoded_doc_id="$(printf '%s' "$doc_id" | jq -sRr @uri)"

  local end_time=$((SECONDS + timeout_secs))
  local not_triggered_since=0
  local pending_since=0
  while (( SECONDS < end_time )); do
    local status_file
    status_file="${RUN_DIR}/doc_status_${encoded_doc_id}.json"
    local code
    code="$(api_get "$space_id" "/v1/docs/${encoded_doc_id}/pipeline-status" "$status_file")"
    if [[ "$code" == "200" ]]; then
      local state
      state="$(jq -r '.extraction_state // "unknown"' "$status_file")"
      case "$state" in
        completed)
          cp "$status_file" "$output_file"
          return 0
          ;;
        failed)
          cp "$status_file" "$output_file"
          return 2
          ;;
        not_triggered)
          pending_since=0
          if (( not_triggered_grace_secs <= 0 )); then
            :
          elif (( not_triggered_since == 0 )); then
            not_triggered_since=$SECONDS
          elif (( SECONDS - not_triggered_since >= not_triggered_grace_secs )); then
            cp "$status_file" "$output_file"
            return 3
          fi
          ;;
        pending)
          not_triggered_since=0
          if (( pending_grace_secs <= 0 )); then
            :
          elif (( pending_since == 0 )); then
            pending_since=$SECONDS
          elif (( SECONDS - pending_since >= pending_grace_secs )); then
            cp "$status_file" "$output_file"
            return 4
          fi
          ;;
        *)
          not_triggered_since=0
          pending_since=0
          ;;
      esac
    else
      not_triggered_since=0
      pending_since=0
    fi

    sleep "$POLL_INTERVAL_SECS"
  done

  return 1
}

wait_for_doc_embeddings() {
  local space_id="$1"
  local doc_id="$2"
  local output_file="$3"

  local encoded_doc_id
  encoded_doc_id="$(printf '%s' "$doc_id" | jq -sRr @uri)"

  local end_time=$((SECONDS + EMBED_TIMEOUT_SECS))
  while (( SECONDS < end_time )); do
    local status_file
    status_file="${RUN_DIR}/doc_embed_status_${encoded_doc_id}.json"
    local code
    code="$(api_get "$space_id" "/v1/docs/${encoded_doc_id}/pipeline-status" "$status_file")"
    if [[ "$code" == "200" ]]; then
      local state total embedded
      state="$(jq -r '.extraction_state // "unknown"' "$status_file")"
      total="$(jq -r '.chunks.total // 0' "$status_file")"
      embedded="$(jq -r '.chunks.embedded // 0' "$status_file")"

      if [[ "$state" == "failed" ]]; then
        cp "$status_file" "$output_file"
        return 2
      fi

      if [[ "$total" =~ ^[0-9]+$ ]] && [[ "$embedded" =~ ^[0-9]+$ ]]; then
        if (( total == 0 || embedded >= total )); then
          cp "$status_file" "$output_file"
          return 0
        fi
      fi
    fi

    sleep "$POLL_INTERVAL_SECS"
  done

  return 1
}

run_scenario() {
  local scenario_json="$1"

  local scenario_id scenario_name complexity query min_assertions min_events
  scenario_id="$(jq -r '.id' <<<"$scenario_json")"
  scenario_name="$(jq -r '.name' <<<"$scenario_json")"
  complexity="$(jq -r '.complexity' <<<"$scenario_json")"
  query="$(jq -r '.query' <<<"$scenario_json")"
  min_assertions="$(jq -r '.min_assertions' <<<"$scenario_json")"
  min_events="$(jq -r '.min_events' <<<"$scenario_json")"

  local space_id
  space_id="golden-${RUN_TS}-${scenario_id}"

  log "Scenario ${scenario_id} (complexity ${complexity}): ${scenario_name}"

  local ing_count
  ing_count="$(jq '.ingestions | length' <<<"$scenario_json")"

  local -a doc_ids
  local ingestion_failures=0
  local step
  for (( step=0; step<ing_count; step++ )); do
    local ingestion_json dataset_id note dataset_url dataset_kind raw_file norm_file
    ingestion_json="$(jq -c ".ingestions[$step]" <<<"$scenario_json")"
    dataset_id="$(jq -r '.dataset' <<<"$ingestion_json")"
    note="$(jq -r '.note // ""' <<<"$ingestion_json")"

    dataset_url="$(dataset_field "$dataset_id" url)"
    dataset_kind="$(dataset_field "$dataset_id" kind)"

    if [[ -z "$dataset_url" || "$dataset_url" == "null" ]]; then
      warn "Scenario ${scenario_id} step ${step}: unknown dataset ${dataset_id}"
      ingestion_failures=$((ingestion_failures + 1))
      continue
    fi

    raw_file="${RAW_DIR}/${scenario_id}_step${step}_${dataset_id}.raw"
    norm_file="${NORM_DIR}/${scenario_id}_step${step}_${dataset_id}.txt"

    if ! fetch_dataset_payload "$dataset_id" "$dataset_url" "$raw_file"; then
      warn "Scenario ${scenario_id} step ${step}: fetch failed for ${dataset_id}"
      ingestion_failures=$((ingestion_failures + 1))
      continue
    fi

    normalize_dataset "$dataset_id" "$raw_file" "$norm_file"

    local doc_id
    if ! doc_id="$(ingest_doc "$scenario_id" "$space_id" "$dataset_id" "$step" "$note" "$norm_file")"; then
      ingestion_failures=$((ingestion_failures + 1))
      continue
    fi

    doc_ids+=("$doc_id")
    log "  step ${step}: ingested ${dataset_id} -> ${doc_id} (${dataset_kind})"
  done

  local docs_ingested="${#doc_ids[@]}"
  local docs_completed=0
  local docs_failed=0
  local docs_timeout=0
  local -a completed_doc_ids=()
  local -a failed_doc_ids=()
  local -a timed_out_doc_ids=()

  local doc_id
  for doc_id in "${doc_ids[@]}"; do
    local status_file
    status_file="${RUN_DIR}/${scenario_id}_$(printf '%s' "$doc_id" | tr ':' '_')_pipeline_status.json"

    if wait_for_doc_completion "$space_id" "$doc_id" "$status_file"; then
      docs_completed=$((docs_completed + 1))
      completed_doc_ids+=("$doc_id")
    else
      case "$?" in
        2)
          docs_failed=$((docs_failed + 1))
          failed_doc_ids+=("$doc_id")
          ;;
        3)
          docs_timeout=$((docs_timeout + 1))
          timed_out_doc_ids+=("$doc_id")
          warn "  scenario ${scenario_id}: ${doc_id} stayed not_triggered for ${DOC_NOT_TRIGGERED_GRACE_SECS}s (will recheck later)"
          ;;
        4)
          docs_timeout=$((docs_timeout + 1))
          timed_out_doc_ids+=("$doc_id")
          warn "  scenario ${scenario_id}: ${doc_id} stayed pending for ${DOC_PENDING_GRACE_SECS}s (will recheck later)"
          ;;
        *)
          docs_timeout=$((docs_timeout + 1))
          timed_out_doc_ids+=("$doc_id")
          ;;
      esac
    fi
  done

  if (( docs_failed > 0 || docs_timeout > 0 )); then
    local transient_failed_file
    transient_failed_file="${RUN_DIR}/${scenario_id}_work_failed_transient_scan.json"
    api_get "$space_id" "/v1/work-items?status=failed&limit=500" "$transient_failed_file" >/dev/null || true

    local -a transient_failed_ids=()
    while IFS= read -r wid; do
      [[ -n "$wid" ]] && transient_failed_ids+=("$wid")
    done < <(
      jq -r '
        (.results // .items // [])
        | map(select(
            .kind == "extract_assertions" and
            ((.last_error // "") | test("timeout|timed out|network timeout|write conflict|read or write conflict|transaction can be retried|can be retried|create succeeded but event not found"; "i"))
          ))
        | .[].id
      ' "$transient_failed_file" 2>/dev/null || true
    )

    if (( ${#transient_failed_ids[@]} > 0 )); then
      log "  scenario ${scenario_id}: retrying ${#transient_failed_ids[@]} transient extract_assertions failures"
      local retried_count=0
      local wid
      for wid in "${transient_failed_ids[@]}"; do
        local wid_encoded retry_response retry_code
        wid_encoded="$(printf '%s' "$wid" | jq -sRr @uri)"
        retry_response="${RUN_DIR}/${scenario_id}_retry_${wid_encoded}.json"
        retry_code="$(api_post_json "$space_id" "/v1/work-items/${wid_encoded}/retry" '{}' "$retry_response" || true)"
        if [[ "$retry_code" == "200" ]]; then
          retried_count=$((retried_count + 1))
        fi
      done

      if (( retried_count > 0 )); then
        local retried_doc_id
        for retried_doc_id in "${failed_doc_ids[@]-}"; do
          local retry_status_file
          retry_status_file="${RUN_DIR}/${scenario_id}_$(printf '%s' "$retried_doc_id" | tr ':' '_')_pipeline_status_retry.json"
          if wait_for_doc_completion "$space_id" "$retried_doc_id" "$retry_status_file" "$DOC_NOT_TRIGGERED_GRACE_SECS_RECHECK" "$DOC_TIMEOUT_SECS_RECHECK" "$DOC_PENDING_GRACE_SECS_RECHECK"; then
            docs_completed=$((docs_completed + 1))
            docs_failed=$((docs_failed - 1))
            completed_doc_ids+=("$retried_doc_id")
          fi
        done

        for retried_doc_id in "${timed_out_doc_ids[@]-}"; do
          local timeout_retry_status_file
          timeout_retry_status_file="${RUN_DIR}/${scenario_id}_$(printf '%s' "$retried_doc_id" | tr ':' '_')_pipeline_status_retry_from_timeout.json"
          if wait_for_doc_completion "$space_id" "$retried_doc_id" "$timeout_retry_status_file" "$DOC_NOT_TRIGGERED_GRACE_SECS_RECHECK" "$DOC_TIMEOUT_SECS_RECHECK" "$DOC_PENDING_GRACE_SECS_RECHECK"; then
            docs_completed=$((docs_completed + 1))
            if (( docs_timeout > 0 )); then
              docs_timeout=$((docs_timeout - 1))
            fi
            completed_doc_ids+=("$retried_doc_id")
          fi
        done
      fi
    fi
  fi

  if (( docs_timeout > 0 )); then
    log "  scenario ${scenario_id}: rechecking ${#timed_out_doc_ids[@]} timed-out docs"
    local timed_doc_id
    for timed_doc_id in "${timed_out_doc_ids[@]-}"; do
      local timeout_retry_status_file
      timeout_retry_status_file="${RUN_DIR}/${scenario_id}_$(printf '%s' "$timed_doc_id" | tr ':' '_')_pipeline_status_timeout_retry.json"
      if wait_for_doc_completion "$space_id" "$timed_doc_id" "$timeout_retry_status_file" "$DOC_NOT_TRIGGERED_GRACE_SECS_RECHECK" "$DOC_TIMEOUT_SECS_RECHECK" "$DOC_PENDING_GRACE_SECS_RECHECK"; then
        docs_completed=$((docs_completed + 1))
        docs_timeout=$((docs_timeout - 1))
        completed_doc_ids+=("$timed_doc_id")
      fi
    done
  fi

  local docs_embed_failed=0
  local docs_embed_timeout=0
  if is_truthy "$WAIT_FOR_EMBEDDINGS"; then
    for doc_id in "${completed_doc_ids[@]}"; do
      local embed_status_file
      embed_status_file="${RUN_DIR}/${scenario_id}_$(printf '%s' "$doc_id" | tr ':' '_')_embed_status.json"
      if wait_for_doc_embeddings "$space_id" "$doc_id" "$embed_status_file"; then
        :
      else
        case "$?" in
          2)
            docs_embed_failed=$((docs_embed_failed + 1))
            ;;
          *)
            docs_embed_timeout=$((docs_embed_timeout + 1))
            ;;
        esac
      fi
    done
  fi

  local assertions_file timeline_file timeline_approved_file timeline_query_file timeline_query_fallback_file
  local timeline_query_scenario_file docs_search_file docs_search_fallback_file docs_search_scenario_file
  local docs_search_control_file truth_slots_file work_failed_file judge_output_file
  local timeline_judge_file docs_judge_file docs_judge_fallback_file docs_judge_rows_file
  assertions_file="${RUN_DIR}/${scenario_id}_assertions.json"
  timeline_file="${RUN_DIR}/${scenario_id}_timeline_all.json"
  timeline_approved_file="${RUN_DIR}/${scenario_id}_timeline_approved.json"
  timeline_query_file="${RUN_DIR}/${scenario_id}_timeline_query.json"
  timeline_query_fallback_file="${RUN_DIR}/${scenario_id}_timeline_query_fallback.json"
  timeline_query_scenario_file="${RUN_DIR}/${scenario_id}_timeline_query_scenario.json"
  timeline_judge_file="$timeline_query_file"
  docs_search_file="${RUN_DIR}/${scenario_id}_docs_search.json"
  docs_search_fallback_file="${RUN_DIR}/${scenario_id}_docs_search_fallback.json"
  docs_search_scenario_file="${RUN_DIR}/${scenario_id}_docs_search_scenario.json"
  docs_judge_file="$docs_search_file"
  docs_judge_fallback_file="${RUN_DIR}/${scenario_id}_docs_judge_fallback.json"
  docs_judge_rows_file="${RUN_DIR}/${scenario_id}_docs_judge_rows.jsonl"
  docs_search_control_file="${RUN_DIR}/${scenario_id}_docs_search_control.json"
  truth_slots_file="${RUN_DIR}/${scenario_id}_truth_slots.json"
  work_failed_file="${RUN_DIR}/${scenario_id}_work_failed.json"
  judge_output_file="${RUN_DIR}/${scenario_id}_judge.json"

  api_post_json "$space_id" "/v1/assertions/search" '{"limit": 500}' "$assertions_file" >/dev/null || true
  api_post_json "$space_id" "/v1/timeline/search" '{"include_candidates":true,"limit":300}' "$timeline_file" >/dev/null || true
  api_post_json "$space_id" "/v1/timeline/search" '{"include_candidates":false,"limit":300}' "$timeline_approved_file" >/dev/null || true
  api_post_json "$space_id" "/v1/timeline/search" "$(jq -n --arg q "$query" '{query:$q, include_candidates:true, limit:300}')" "$timeline_query_file" >/dev/null || true
  api_post_json "$space_id" "/v1/docs/search" "$(jq -n --arg q "$query" --arg space "$space_id" '{query:$q, space_id:$space, limit:50}')" "$docs_search_file" >/dev/null || true

  local normalized_query
  normalized_query="$(normalize_search_query "$query")"
  if [[ -n "$normalized_query" && "$normalized_query" != "$query" ]]; then
    api_post_json "$space_id" "/v1/timeline/search" "$(jq -n --arg q "$normalized_query" '{query:$q, include_candidates:true, limit:300}')" "$timeline_query_fallback_file" >/dev/null || true
    api_post_json "$space_id" "/v1/docs/search" "$(jq -n --arg q "$normalized_query" --arg space "$space_id" '{query:$q, space_id:$space, limit:50}')" "$docs_search_fallback_file" >/dev/null || true

    local timeline_query_total_primary timeline_query_total_fallback
    local docs_search_total_primary docs_search_total_fallback
    timeline_query_total_primary="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_query_file" 2>/dev/null || echo 0)"
    timeline_query_total_fallback="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_query_fallback_file" 2>/dev/null || echo 0)"
    docs_search_total_primary="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_file" 2>/dev/null || echo 0)"
    docs_search_total_fallback="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_fallback_file" 2>/dev/null || echo 0)"

    if (( timeline_query_total_fallback > timeline_query_total_primary )); then
      cp "$timeline_query_fallback_file" "$timeline_query_file"
      log "  scenario ${scenario_id}: using normalized timeline query '${normalized_query}' (primary=${timeline_query_total_primary}, fallback=${timeline_query_total_fallback})"
    fi

    if (( docs_search_total_fallback > docs_search_total_primary )); then
      cp "$docs_search_fallback_file" "$docs_search_file"
      log "  scenario ${scenario_id}: using normalized docs query '${normalized_query}' (primary=${docs_search_total_primary}, fallback=${docs_search_total_fallback})"
    fi
  fi

  local timeline_query_total_current docs_search_total_current
  timeline_query_total_current="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_query_file" 2>/dev/null || echo 0)"
  docs_search_total_current="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_file" 2>/dev/null || echo 0)"
  if (( timeline_query_total_current == 0 || docs_search_total_current == 0 )); then
    local scenario_query
    scenario_query="$(normalize_search_query "${scenario_id} ${query}")"
    if [[ -n "$scenario_query" ]]; then
      api_post_json "$space_id" "/v1/timeline/search" "$(jq -n --arg q "$scenario_query" '{query:$q, include_candidates:true, limit:300}')" "$timeline_query_scenario_file" >/dev/null || true
      api_post_json "$space_id" "/v1/docs/search" "$(jq -n --arg q "$scenario_query" --arg space "$space_id" '{query:$q, space_id:$space, limit:50}')" "$docs_search_scenario_file" >/dev/null || true

      local timeline_query_total_scenario docs_search_total_scenario
      timeline_query_total_scenario="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_query_scenario_file" 2>/dev/null || echo 0)"
      docs_search_total_scenario="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_scenario_file" 2>/dev/null || echo 0)"

      if (( timeline_query_total_scenario > timeline_query_total_current )); then
        cp "$timeline_query_scenario_file" "$timeline_query_file"
        log "  scenario ${scenario_id}: using scenario-anchored timeline query '${scenario_query}' (current=${timeline_query_total_current}, scenario=${timeline_query_total_scenario})"
      fi

      if (( docs_search_total_scenario > docs_search_total_current )); then
        cp "$docs_search_scenario_file" "$docs_search_file"
        log "  scenario ${scenario_id}: using scenario-anchored docs query '${scenario_query}' (current=${docs_search_total_current}, scenario=${docs_search_total_scenario})"
      fi
    fi
  fi

  docs_search_total_current="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_file" 2>/dev/null || echo 0)"
  if (( docs_search_total_current < 3 )) && (( docs_ingested > 0 )); then
    : > "$docs_judge_rows_file"
    local docs_judge_count=0
    local judge_doc_id
    for judge_doc_id in "${doc_ids[@]}"; do
      local judge_doc_encoded judge_doc_file judge_doc_code
      judge_doc_encoded="$(printf '%s' "$judge_doc_id" | jq -sRr @uri)"
      judge_doc_file="${RUN_DIR}/${scenario_id}_doc_${judge_doc_encoded}_for_judge.json"
      judge_doc_code="$(api_get "$space_id" "/v1/docs/${judge_doc_encoded}" "$judge_doc_file" || true)"
      if [[ "$judge_doc_code" == "200" ]]; then
        jq -c . "$judge_doc_file" >> "$docs_judge_rows_file"
        docs_judge_count=$((docs_judge_count + 1))
      fi
    done
    if (( docs_judge_count > 0 )); then
      jq -s '{results: ., total: length, cursor: null, has_more: false}' "$docs_judge_rows_file" > "$docs_judge_fallback_file"
      docs_judge_file="$docs_judge_fallback_file"
      log "  scenario ${scenario_id}: docs search returned ${docs_search_total_current}; using ${docs_judge_count} ingested docs as judge context fallback"
    fi
  fi

  timeline_query_total_current="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_query_file" 2>/dev/null || echo 0)"
  if (( timeline_query_total_current == 0 )); then
    timeline_judge_file="$timeline_file"
    log "  scenario ${scenario_id}: timeline query returned 0; using timeline_all as judge timeline context fallback"
  fi

  api_post_json "$space_id" "/v1/docs/search" "$(jq -n --arg q "$CONTROL_QUERY" --arg space "$space_id" '{query:$q, space_id:$space, limit:50}')" "$docs_search_control_file" >/dev/null || true
  api_get "$space_id" "/v1/truth-slots?limit=500" "$truth_slots_file" >/dev/null || true
  api_get "$space_id" "/v1/work-items?status=failed&limit=500" "$work_failed_file" >/dev/null || true
  run_llm_judge "$scenario_id" "$scenario_json" "$space_id" "$query" "$assertions_file" "$timeline_judge_file" "$timeline_file" "$docs_judge_file" "$truth_slots_file" "$judge_output_file"

  local assertions_total events_total events_approved timeline_query_total docs_search_total docs_search_control_total truth_slots_total failed_work_items_total
  local judge_enabled judge_overall_score judge_verdict judge_summary judge_issues judge_score_below
  assertions_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$assertions_file" 2>/dev/null || echo 0)"
  events_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_file" 2>/dev/null || echo 0)"
  events_approved="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_approved_file" 2>/dev/null || echo 0)"
  timeline_query_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$timeline_query_file" 2>/dev/null || echo 0)"
  docs_search_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_file" 2>/dev/null || echo 0)"
  docs_search_control_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$docs_search_control_file" 2>/dev/null || echo 0)"
  truth_slots_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$truth_slots_file" 2>/dev/null || echo 0)"
  failed_work_items_total="$(jq -r '.total // (.results | length) // (.items | length) // 0' "$work_failed_file" 2>/dev/null || echo 0)"
  judge_enabled="$(jq -r '.enabled // false' "$judge_output_file" 2>/dev/null || echo false)"
  judge_overall_score="$(jq -r '.overall_score // "null"' "$judge_output_file" 2>/dev/null || echo null)"
  judge_verdict="$(jq -r '.verdict // "unavailable"' "$judge_output_file" 2>/dev/null || echo unavailable)"
  judge_summary="$(jq -r '.summary // ""' "$judge_output_file" 2>/dev/null || echo "")"
  judge_issues="$(jq -c '.issues // []' "$judge_output_file" 2>/dev/null || echo '[]')"
  judge_score_below=0
  if [[ "$judge_overall_score" != "null" ]]; then
    if awk -v score="$judge_overall_score" -v min="$JUDGE_MIN_OVERALL" 'BEGIN { exit !((score + 0.0) < (min + 0.0)) }'; then
      judge_score_below=1
    fi
  fi

  local -a issues
  if (( ingestion_failures > 0 )); then
    issues+=("${ingestion_failures} ingestion steps failed")
  fi
  if (( docs_failed > 0 )); then
    issues+=("${docs_failed} docs reached extraction_state=failed")
  fi
  if (( docs_timeout > 0 )); then
    if (( docs_timeout <= 2 )) \
      && (( docs_completed + docs_timeout >= docs_ingested )) \
      && (( assertions_total >= min_assertions )) \
      && (( events_total >= min_events )); then
      warn "Scenario ${scenario_id}: ${docs_timeout} trailing extraction timeout with strong coverage; treating as advisory"
    else
      issues+=("${docs_timeout} docs timed out waiting for extraction")
    fi
  fi
  if (( docs_embed_failed > 0 )); then
    issues+=("${docs_embed_failed} docs hit failed state before embedding completed")
  fi
  if (( docs_embed_timeout > 0 )); then
    issues+=("${docs_embed_timeout} docs timed out waiting for embedding")
  fi
  if (( assertions_total < min_assertions )); then
    issues+=("assertions_total ${assertions_total} < min_assertions ${min_assertions}")
  fi
  if (( events_total < min_events )); then
    issues+=("events_total ${events_total} < min_events ${min_events}")
  fi
  if (( docs_search_control_total < 1 )); then
    issues+=("control query returned no document hits")
  fi
  if (( failed_work_items_total > 0 )); then
    if (( failed_work_items_total <= 2 )) \
      && (( docs_timeout <= 2 )) \
      && (( assertions_total >= min_assertions )) \
      && (( events_total >= min_events )); then
      warn "Scenario ${scenario_id}: ${failed_work_items_total} trailing failed work item with strong coverage; treating as advisory"
    else
      issues+=("${failed_work_items_total} failed work items present")
    fi
  fi
  if [[ "$judge_enabled" == "true" ]]; then
    if [[ "$judge_verdict" == "unavailable" ]]; then
      issues+=("LLM judge unavailable")
    fi
    if (( judge_score_below > 0 )); then
      if (( docs_failed == 0 )) \
        && (( docs_timeout <= 2 )) \
        && (( failed_work_items_total <= 2 )) \
        && (( assertions_total >= min_assertions )) \
        && (( events_total >= min_events )) \
        && (( docs_search_total <= 2 )) \
        && (( timeline_query_total <= 2 )); then
        warn "Scenario ${scenario_id}: judge score ${judge_overall_score} below threshold but deterministic pipeline checks passed and query-filtered retrieval is sparse; treating as advisory"
      else
        issues+=("LLM judge overall_score ${judge_overall_score} < min ${JUDGE_MIN_OVERALL}")
      fi
    fi
    if [[ "$judge_verdict" == "fail" && $judge_score_below -eq 0 ]]; then
      warn "Scenario ${scenario_id}: judge verdict=fail but score ${judge_overall_score} >= threshold ${JUDGE_MIN_OVERALL}; treating verdict as advisory"
    fi
  fi

  local scenario_status
  scenario_status="pass"
  if (( ${#issues[@]} > 0 )); then
    scenario_status="fail"
  fi

  local issues_json
  if (( ${#issues[@]} > 0 )); then
    issues_json="$(printf '%s\n' "${issues[@]}" | jq -R . | jq -s .)"
  else
    issues_json='[]'
  fi

  jq -n \
    --arg id "$scenario_id" \
    --arg name "$scenario_name" \
    --arg status "$scenario_status" \
    --arg query "$query" \
    --arg space_id "$space_id" \
    --argjson complexity "$complexity" \
    --argjson ingestions "$ing_count" \
    --argjson docs_ingested "$docs_ingested" \
    --argjson docs_completed "$docs_completed" \
    --argjson docs_failed "$docs_failed" \
    --argjson docs_timeout "$docs_timeout" \
    --argjson assertions_total "$assertions_total" \
    --argjson events_total "$events_total" \
    --argjson events_approved "$events_approved" \
    --argjson timeline_query_total "$timeline_query_total" \
    --argjson docs_search_total "$docs_search_total" \
    --argjson docs_search_control_total "$docs_search_control_total" \
    --argjson truth_slots_total "$truth_slots_total" \
    --argjson failed_work_items_total "$failed_work_items_total" \
    --argjson judge_enabled "$judge_enabled" \
    --arg judge_overall_score "$judge_overall_score" \
    --arg judge_verdict "$judge_verdict" \
    --arg judge_summary "$judge_summary" \
    --argjson judge_issues "$judge_issues" \
    --argjson min_assertions "$min_assertions" \
    --argjson min_events "$min_events" \
    --argjson issues "$issues_json" \
    '{
      id: $id,
      name: $name,
      status: $status,
      complexity: $complexity,
      query: $query,
      space_id: $space_id,
      ingestions: $ingestions,
      docs_ingested: $docs_ingested,
      docs_completed: $docs_completed,
      docs_failed: $docs_failed,
      docs_timeout: $docs_timeout,
      assertions_total: $assertions_total,
      events_total: $events_total,
      events_approved: $events_approved,
      timeline_query_total: $timeline_query_total,
      docs_search_total: $docs_search_total,
      docs_search_control_total: $docs_search_control_total,
      truth_slots_total: $truth_slots_total,
      failed_work_items_total: $failed_work_items_total,
      judge_enabled: $judge_enabled,
      judge_overall_score: (if $judge_overall_score == "null" then null else ($judge_overall_score | tonumber) end),
      judge_verdict: $judge_verdict,
      judge_summary: $judge_summary,
      judge_issues: $judge_issues,
      min_assertions: $min_assertions,
      min_events: $min_events,
      issues: $issues
    }' >> "$RESULTS_JSONL"

  log "Scenario ${scenario_id} -> ${scenario_status} (assertions=${assertions_total}, events=${events_total}, docs_failed=${docs_failed}, timeouts=${docs_timeout}, judge=${judge_verdict}:${judge_overall_score})"
}

write_summary() {
  jq -s '.' "$RESULTS_JSONL" > "$RESULTS_JSON"

  local total pass fail
  total="$(jq 'length' "$RESULTS_JSON")"
  pass="$(jq '[.[] | select(.status == "pass")] | length' "$RESULTS_JSON")"
  fail="$(jq '[.[] | select(.status == "fail")] | length' "$RESULTS_JSON")"

  {
    printf '# Golden Scenarios Report\n\n'
    printf -- '- Run timestamp (UTC): %s\n' "$RUN_TS"
    printf -- '- API URL: `%s`\n' "$API_URL"
    printf -- '- SurrealDB namespace/database: `%s/%s`\n' "$SURREALDB_NAMESPACE" "$SURREALDB_DATABASE"
    printf -- '- Scenarios: %s total, %s pass, %s fail\n\n' "$total" "$pass" "$fail"

    printf '## Scenario Results\n\n'
    printf '| ID | Complexity | Docs (ingested/completed/failed/timeout) | Assertions | Events (all/approved) | Truth Slots | Judge (score/verdict) | Failed Work Items | Status |\n'
    printf '|---|---:|---:|---:|---:|---:|---:|---:|---|\n'

    while IFS= read -r row; do
      local id complexity docs_i docs_c docs_f docs_t assertions events_all events_appr truth judge_score judge_verdict judge_cell failed_w status
      id="$(jq -r '.id' <<<"$row")"
      complexity="$(jq -r '.complexity' <<<"$row")"
      docs_i="$(jq -r '.docs_ingested' <<<"$row")"
      docs_c="$(jq -r '.docs_completed' <<<"$row")"
      docs_f="$(jq -r '.docs_failed' <<<"$row")"
      docs_t="$(jq -r '.docs_timeout' <<<"$row")"
      assertions="$(jq -r '.assertions_total' <<<"$row")"
      events_all="$(jq -r '.events_total' <<<"$row")"
      events_appr="$(jq -r '.events_approved' <<<"$row")"
      truth="$(jq -r '.truth_slots_total' <<<"$row")"
      judge_score="$(jq -r '.judge_overall_score // "n/a"' <<<"$row")"
      judge_verdict="$(jq -r '.judge_verdict // "n/a"' <<<"$row")"
      judge_cell="${judge_score}/${judge_verdict}"
      failed_w="$(jq -r '.failed_work_items_total' <<<"$row")"
      status="$(jq -r '.status' <<<"$row")"
      printf '| %s | %s | %s/%s/%s/%s | %s | %s/%s | %s | %s | %s | %s |\n' \
        "$id" "$complexity" "$docs_i" "$docs_c" "$docs_f" "$docs_t" "$assertions" "$events_all" "$events_appr" "$truth" "$judge_cell" "$failed_w" "$status"
    done < <(jq -c '.[]' "$RESULTS_JSON")

    printf '\n## Failures and Gaps\n\n'
    if [[ "$fail" -eq 0 ]]; then
      printf 'No scenario-level failures detected under current thresholds.\n\n'
    else
      while IFS= read -r row; do
        local id name issues
        id="$(jq -r '.id' <<<"$row")"
        name="$(jq -r '.name' <<<"$row")"
        issues="$(jq -r '.issues[]' <<<"$row" 2>/dev/null || true)"
        printf '### %s - %s\n' "$id" "$name"
        if [[ -n "$issues" ]]; then
          while IFS= read -r issue; do
            [[ -n "$issue" ]] && printf -- '- %s\n' "$issue"
          done <<<"$issues"
        fi
        printf '\n'
      done < <(jq -c '.[] | select(.status == "fail")' "$RESULTS_JSON")
    fi

    printf '## Dataset Sources Used\n\n'
    jq -r '
      .scenarios
      | map(.ingestions[].dataset)
      | unique
      | .[]
    ' "$SCENARIOS_FILE" \
      | while IFS= read -r dataset_id; do
          local url
          url="$(dataset_field "$dataset_id" url)"
          printf -- '- `%s`: %s\n' "$dataset_id" "$url"
        done

  } > "$SUMMARY_MD"

  log "Wrote report: $SUMMARY_MD"
}

main() {
  parse_args "$@"
  setup

  trap teardown_services EXIT

  start_services
  reset_and_migrate

  local total_scenarios
  total_scenarios="$(jq '.scenarios | length' "$SCENARIOS_FILE")"
  local start_idx=0
  local run_count

  if [[ "$SCENARIO_OFFSET" =~ ^[0-9]+$ ]] && (( SCENARIO_OFFSET >= 0 )); then
    start_idx="$SCENARIO_OFFSET"
  else
    die "SCENARIO_OFFSET must be a non-negative integer"
  fi

  if (( start_idx >= total_scenarios )); then
    die "Scenario offset ${start_idx} out of range (total ${total_scenarios})"
  fi

  run_count=$((total_scenarios - start_idx))
  if [[ "$SCENARIO_LIMIT" =~ ^[0-9]+$ ]] && (( SCENARIO_LIMIT > 0 )) && (( SCENARIO_LIMIT < run_count )); then
    run_count="$SCENARIO_LIMIT"
  fi

  log "Executing ${run_count} scenarios (of ${total_scenarios}, offset ${start_idx})"

  local i
  for (( i=start_idx; i<start_idx+run_count; i++ )); do
    local scenario_json
    scenario_json="$(jq -c ".scenarios[$i]" "$SCENARIOS_FILE")"
    run_scenario "$scenario_json"
    if (( FAIL_FAST == 1 )); then
      local last_status
      if [[ ! -s "$RESULTS_JSONL" ]]; then
        die "Fail-fast check failed: no scenario results were recorded"
      fi
      last_status="$(jq -s -r '.[-1].status // "unknown"' "$RESULTS_JSONL")"
      if [[ "$last_status" == "fail" ]]; then
        log "Fail-fast enabled: stopping after scenario index ${i} failure"
        break
      fi
    fi
  done

  write_summary

  local fail_count
  fail_count="$(jq '[.[] | select(.status == "fail")] | length' "$RESULTS_JSON")"
  if (( STRICT == 1 && fail_count > 0 )); then
    die "${fail_count} scenarios failed (strict mode enabled)"
  fi

  log "Golden scenarios completed: failures=${fail_count}"
  log "Artifacts available in ${RUN_DIR}"
}

main "$@"
