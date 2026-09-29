# Golden Scenarios

This suite runs **20 escalating, deliberately convoluted end-to-end scenarios** against a live Elephant stack:

- Dockerized SurrealDB + Elephant API (supervisor runs pipeline)
- Live external dataset fetches (public APIs/RSS)
- Live LLM extraction (`ANTHROPIC_API_KEY`) and embeddings (`OPENAI_API_KEY`)
- Real ingestion (`/v1/docs/ingest`) + polling pipeline completion
- Real query checks (`/v1/docs/search`, `/v1/assertions/search`, `/v1/timeline/search`, `/v1/truth-slots`)
- Metadata-driven extraction routing (`extraction_skill`, `extraction_purpose`) exercised per ingested document
- LLM judge scoring per scenario (relevance, grounding, temporal correctness, contradiction handling)

## Files

- `datasets.json`: public data sources used by scenarios.
- `scenarios.json`: 20 scenarios, ordered by increasing complexity.
- `run_golden_scenarios.sh`: orchestrator + reporter.

## Run

From repo root:

```bash
export ANTHROPIC_API_KEY=...
export OPENAI_API_KEY=...
./tests/golden_scenarios/run_golden_scenarios.sh
```

Useful options:

```bash
# Quick smoke (first 3 scenarios)
./tests/golden_scenarios/run_golden_scenarios.sh --limit 3

# Fail CI if any scenario fails thresholds
./tests/golden_scenarios/run_golden_scenarios.sh --strict

# Use already-running services
./tests/golden_scenarios/run_golden_scenarios.sh --no-docker

# Tear down docker services when finished
./tests/golden_scenarios/run_golden_scenarios.sh --teardown
```

Judge configuration:

```bash
# Disable judge (not recommended for quality checks)
JUDGE_ENABLED=false ./tests/golden_scenarios/run_golden_scenarios.sh

# Override judge model and minimum passing score
JUDGE_MODEL=gpt-4o-mini JUDGE_MIN_OVERALL=0.70 ./tests/golden_scenarios/run_golden_scenarios.sh --strict
```

## Outputs

Each run creates:

- `tests/golden_scenarios/output/<timestamp>/results.jsonl`
- `tests/golden_scenarios/output/<timestamp>/results.json`
- `tests/golden_scenarios/output/<timestamp>/summary.md`
- raw/normalized source snapshots and per-scenario API payloads/responses.

## Scenario Design

The scenarios intentionally grow in complexity:

1. Starts convoluted: scenario 1 already uses 3 interleaved ingestions with alias drift and contradictory updates.
2. Escalates continuously: each subsequent scenario adds more ingestion steps, more reversals, and more cross-domain collisions.
3. Ends in hypergraph-like stress: late scenarios combine repeated fetches, chronology inversions, and supersession loops.

Current ingestion fan-in ramps from 3 documents (early scenarios) up to 14 documents (scenario 20).

The suite is built to surface dynamic system issues, especially around:

- event extraction quality,
- candidate/approved visibility dynamics,
- conflict grouping behavior,
- extraction latency/timeouts,
- failed work-item accumulation under load,
- semantic retrieval quality via LLM-graded evaluation.
