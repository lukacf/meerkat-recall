#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export PIPELINE_BUDGET_DAILY_ITEMS_GRAPH=5
PYTHONUNBUFFERED=1 python3 scripts/scenario_5_budget.py
