#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

has_rg=0
if command -v rg >/dev/null 2>&1; then
  has_rg=1
fi

contains_fixed() {
  local needle="$1"
  local file="$2"
  if [[ "$has_rg" == "1" ]]; then
    rg -q --fixed-strings -- "$needle" "$file"
  else
    grep -Fq -- "$needle" "$file"
  fi
}

find_fixed_rs_hits() {
  local needle="$1"
  if [[ "$has_rg" == "1" ]]; then
    rg -n --fixed-strings -g '*.rs' -- "$needle" crates bin || true
  else
    grep -RFn --include='*.rs' -- "$needle" crates bin || true
  fi
}

ACTIVE_DOC="docs/active-architecture.md"
NON_ACTIVE_DOC="docs/non-active-catalog.md"
PIPELINE_MAIN="bin/elephant-pipeline/src/main.rs"
MCP_HANDLERS="crates/mcp/src/handlers/mod.rs"
MCP_PROFILE="crates/mcp/src/profile.rs"
SCHEDULER_MOD="crates/pipeline/src/scheduler/mod.rs"

if [[ ! -f "$ACTIVE_DOC" ]]; then
  echo "missing $ACTIVE_DOC"
  exit 1
fi
if [[ ! -f "$NON_ACTIVE_DOC" ]]; then
  echo "missing $NON_ACTIVE_DOC"
  exit 1
fi

required_work_kinds=(
  ingest_source_stub
  triage_source
  ingest_metadata
  upgrade_ingest_mode
  render_doc
  plan_extraction
  extract_assertions
  validate_artifacts
  commit_artifacts
  validate_resolution_patches
  commit_resolution_patches
  execute_reprocess_job
  tm_detect_conflicts
  tm_resolve_slot
  tm_commit
  tm_recompute
  freshness_sweep
  reconcile_living_wiki
)

for kind in "${required_work_kinds[@]}"; do
  if ! contains_fixed "\`$kind\`" "$ACTIVE_DOC"; then
    echo "active architecture doc missing work kind: $kind"
    exit 1
  fi
  if ! contains_fixed "$kind" "$PIPELINE_MAIN"; then
    echo "pipeline main missing work kind reference: $kind"
    exit 1
  fi
done

legacy_disallowed_in_dispatch=(
  COMMIT_ASSERTIONS_KIND
  RESOLVE_ASSERTIONS_KIND
  SYNTHESIZE_RELS_ATTRS_KIND
  EXTRACT_MATCH_KEYS_KIND
)

match_block="$(awk '/match item.kind.as_str\(\)/, /other => \{/' "$PIPELINE_MAIN")"
for token in "${legacy_disallowed_in_dispatch[@]}"; do
  if grep -q "$token =>" <<<"$match_block"; then
    echo "legacy work kind appears in active dispatch match: $token"
    exit 1
  fi
done

legacy_superseded=(
  commit_assertions
  resolve_assertions
  synthesize_rels_attrs
  extract_match_keys
  resolution_sweep
  resolve_contradiction
)

planned_non_active=(
  reembed
  rebuild_indexes
  snapshot_export
)

for token in "${legacy_superseded[@]}"; do
  if ! contains_fixed "\`$token\`" "$ACTIVE_DOC"; then
    echo "active architecture doc missing legacy token: $token"
    exit 1
  fi
  if ! contains_fixed "\`$token\`" "$NON_ACTIVE_DOC"; then
    echo "non-active catalog missing legacy token: $token"
    exit 1
  fi
done

legacy_runtime_tokens=(
  COMMIT_ASSERTIONS_KIND
  RESOLVE_ASSERTIONS_KIND
  SYNTHESIZE_RELS_ATTRS_KIND
  EXTRACT_MATCH_KEYS_KIND
  RESOLUTION_SWEEP_KIND
  RESOLVE_CONTRADICTION_KIND
  execute_commit_assertions
  execute_resolve_assertions_work_kind
  execute_synthesize_rels_attrs_work_kind
  execute_extract_match_keys
)

for token in "${legacy_runtime_tokens[@]}"; do
  hits="$(find_fixed_rs_hits "$token")"
  if [[ -n "$hits" ]]; then
    echo "legacy runtime reference detected: $token"
    printf "%s\n" "$hits"
    exit 1
  fi
done

for token in "${planned_non_active[@]}"; do
  if ! contains_fixed "\`$token\`" "$ACTIVE_DOC"; then
    echo "active architecture doc missing planned token: $token"
    exit 1
  fi
  if ! contains_fixed "\`$token\`" "$NON_ACTIVE_DOC"; then
    echo "non-active catalog missing planned token: $token"
    exit 1
  fi
done

if contains_fixed "pub mod legacy" "$SCHEDULER_MOD"; then
  echo "scheduler module must not expose pub mod legacy"
  exit 1
fi
if ! contains_fixed "pub mod experimental" "$SCHEDULER_MOD"; then
  echo "scheduler module missing namespace: pub mod experimental"
  exit 1
fi

full_count="$({
  awk '
    /pub fn create_dispatcher_with_context\(/ {s=1; next}
    /pub fn create_default_dispatcher\(/ {s=0}
    s && /dispatcher\.register\(/ {c++}
    s && /register_capabilities_handler\(/ {c++}
    END {print c+0}
  ' "$MCP_HANDLERS"
})"

default_dispatcher_count="$({
  awk '
    /pub fn create_default_dispatcher_with_context\(/ {s=1; next}
    /pub fn create_agent_dispatcher\(/ {s=0}
    s && /dispatcher\.register\(/ {c++}
    END {print c+0}
  ' "$MCP_HANDLERS"
})"

agent_dispatcher_count="$({
  awk '
    /pub fn create_agent_dispatcher_with_context\(/ {s=1; next}
    /pub fn create_maintenance_dispatcher\(/ {s=0}
    s && /dispatcher\.register\(/ {c++}
    s && /register_capabilities_handler\(/ {c++}
    END {print c+0}
  ' "$MCP_HANDLERS"
})"

maintenance_dispatcher_count="$({
  awk '
    /pub fn create_maintenance_dispatcher_with_context\(/ {s=1; next}
    /^\}/ {if (s==1) depth_close++}
    s && /dispatcher\.register\(/ {c++}
    s && /register_capabilities_handler\(/ {c++}
    s && /dispatcher$/ {s=0}
    END {print c+0}
  ' "$MCP_HANDLERS"
})"

array_len() {
  local const_name="$1"
  awk -v name="$const_name" '
    $0 ~ ("pub const " name ": &\\[&str\\] = &\\[") {s=1; next}
    s && /\];/ {s=0}
    s {
      if ($0 ~ /"[^"]+"/) c++
    }
    END {print c+0}
  ' "$MCP_PROFILE"
}

default_profile_count="$(array_len DEFAULT_TOOLS)"
agent_profile_count="$(array_len AGENT_TOOLS)"
maintenance_profile_count="$(array_len MAINTENANCE_TOOLS)"

[[ "$full_count" == "100" ]] || { echo "expected full dispatcher count 100, got $full_count"; exit 1; }
[[ "$default_dispatcher_count" == "26" ]] || { echo "expected default dispatcher count 26, got $default_dispatcher_count"; exit 1; }
[[ "$agent_dispatcher_count" == "27" ]] || { echo "expected agent dispatcher count 27, got $agent_dispatcher_count"; exit 1; }
[[ "$maintenance_dispatcher_count" == "46" ]] || { echo "expected maintenance dispatcher count 46, got $maintenance_dispatcher_count"; exit 1; }

[[ "$default_profile_count" == "$default_dispatcher_count" ]] || {
  echo "default profile count $default_profile_count != dispatcher count $default_dispatcher_count"
  exit 1
}
[[ "$agent_profile_count" == "$agent_dispatcher_count" ]] || {
  echo "agent profile count $agent_profile_count != dispatcher count $agent_dispatcher_count"
  exit 1
}
[[ "$maintenance_profile_count" == "$maintenance_dispatcher_count" ]] || {
  echo "maintenance profile count $maintenance_profile_count != dispatcher count $maintenance_dispatcher_count"
  exit 1
}

echo "verify_active_surface: ok"
