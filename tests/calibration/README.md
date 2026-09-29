# elephant-calibration

Prompt calibration harness for Elephant's two pure-data LLM entrypoints:

- **`tm_adjudication`** — `AgenticSlotAdjudicator` (truth-maintenance slot
  verdicts), prompt at `crates/pipeline/prompts/tm_adjudication.txt`.
- **`entity_disambiguation`** — `AgenticPromotionDisambiguator` (entity reuse
  at promotion), prompt at `crates/pipeline/prompts/promotion_disambiguation.txt`.

Each suite feeds labeled cases from `cases/*.json` to the production
entrypoint, scores the structured verdicts, prints a per-case table, and
writes `<target>/calibration/<suite>/results.json` (the runner prints the
absolute path; under `scripts/repo-cargo` the target dir lives in the shared
cache, not the repo).

## Running

The real-LLM runners are `#[ignore]`d and skip cleanly when
`ANTHROPIC_API_KEY` is unset, so keyless `cargo integration` / `cargo
test-e2e` lanes pass.

```bash
# Both suites
ANTHROPIC_API_KEY=... ./scripts/repo-cargo test -p elephant-calibration -- --ignored --nocapture

# One suite
ANTHROPIC_API_KEY=... ./scripts/repo-cargo test -p elephant-calibration calibrate_tm_adjudication -- --ignored --nocapture

# Different model (default: the production default, claude-sonnet-4-5)
CALIBRATION_MODEL=claude-opus-4-8 ANTHROPIC_API_KEY=... ./scripts/repo-cargo test -p elephant-calibration -- --ignored --nocapture

# Offline tests only (case validation, scoring, serialization; no key needed)
./scripts/repo-cargo test -p elephant-calibration
```

Cases run with at most 4 concurrent LLM calls. Each suite asserts a hard-pass
rate floor of 0.8 and lists the failing case ids on violation.

## Scoring

A case **hard-passes** when the verdict matches the primary `expected` block
or any `acceptable_alternatives` block on every hard field the block
specifies:

- `decision` — verdict class (`corroborate`, `single_winner`, `coexist`,
  `supersede`, `needs_review`; disambiguation: `action` + `entity_id`)
- `winner_id` — exact claim id
- `superseded_ids` — set equality (`[]` asserts nothing was superseded)

**Soft** fields are recorded in `results.json` but never gate the floor:

- `equivalence_partition` — set-of-sets equality with `equivalent_groups`
- `proposed_cardinality` — the verdict's proposed semantics cardinality
- `superseded_year_by_claim` — the year of each superseded claim's
  `valid_until`

## Iterating on a prompt

1. Save the current baseline: copy `<target>/calibration/<suite>/results.json`
   aside (or just note its `prompt_sha256`).
2. Edit the prompt template under `crates/pipeline/prompts/`.
3. Rerun the suite (the template is compiled in via `include_str!`, so a
   rebuild picks it up automatically).
4. Diff the new `results.json` against the baseline. `prompt_sha256` is the
   SHA-256 of the template file bytes, so runs are attributable to exact
   prompt versions.

## Adding cases

Append to `cases/tm_adjudication.json` or `cases/entity_disambiguation.json`.
Schema per case:

```json
{
  "id": "unique_snake_case_id",
  "description": "what this exercises",
  "input": { "...": "serde mirror of the entrypoint's request struct" },
  "expected": { "decision": "...", "winner_id": "...", "...": "..." },
  "acceptable_alternatives": [ { "decision": "needs_review" } ],
  "tags": ["coverage", "labels"]
}
```

Omit a field in an expectation block to leave it unscored. The offline test
`tm_case_suite_parses_and_validates` (run on every plain `cargo test`) rejects
duplicate ids, references to unknown claim/candidate ids, malformed
partitions, and unknown labels — update its expected case counts when adding
cases.
