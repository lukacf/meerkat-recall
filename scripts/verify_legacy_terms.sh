#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

has_rg=0
if command -v rg >/dev/null 2>&1; then
  has_rg=1
fi

files=(
  README.md
  docs
  docs/active-architecture.md
  bin/elephant-api/src/mcp.rs
)

# Local agent guides are optional and are excluded from source archives.
for guide in .codex/skills/elephant-dev .codex/skills/elephant-mcp; do
  if [[ -d "$guide" ]]; then
    files+=("$guide")
  fi
done

hits_file="$(mktemp)"
trap 'rm -f "$hits_file"' EXIT

forbidden=(
  '/v1/facts'
  'Create a fact'
  'candidate fact'
  'facts tools'
  'Promotes candidates to facts'
)

for pattern in "${forbidden[@]}"; do
  scan_status=0
  if [[ "$has_rg" == "1" ]]; then
    rg -n --fixed-strings -- "$pattern" "${files[@]}" >"$hits_file" || scan_status=$?
  else
    grep -rnF -- "$pattern" "${files[@]}" >"$hits_file" || scan_status=$?
  fi
  if (( scan_status > 1 )); then
    echo "legacy term scan failed" >&2
    exit "$scan_status"
  fi
  if [[ -s "$hits_file" ]]; then
    echo "forbidden legacy term found: $pattern"
    cat "$hits_file"
    exit 1
  fi
done

echo "verify_legacy_terms: ok"
