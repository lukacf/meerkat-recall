# Design: Value-Aware Truth Maintenance with Reachable LLM Adjudication

Status: historical design record, implemented and superseded by
[Active architecture](../active-architecture.md).

The problem statement and source line numbers below describe the pre-change
system. They are retained to explain the design, not as current bug reports.

## Historical problem

Truth maintenance is 100% deterministic in practice:

1. **Detection is structural.** `execute_tm_detect_conflicts_with_storage`
   declares a conflict iff `claim_count > 1` rows share a slot key
   (truth_maintenance/mod.rs:1470 rels, :1586 attributes). Claim values are
   never compared:
   - Two sources asserting the identical attribute value produce a fake
     "conflict"; resolution then marks one Superseded (and closes its validity
     window), destroying corroboration.
   - Rel slot keys include the object (`compute_slot_key` :689), so
     contradictory objects for the same subject+predicate (`works_at ExampleCo` vs
     `works_at ExampleLabs`) can NEVER conflict. `PredicateSemantics.cardinality`
     is read by no live code (sole read: the dead reasoner prompt :1976).
2. **The LLM tie-break is provably dead code.** `should_use_reasoner` (:2380)
   requires a `NeedsReview` deterministic result; `RequireMoreEvidence` (the
   only policy producing NeedsReview for non-empty claims) early-returns before
   the reasoner (:2407); the default `AllowParallel` early-returns before the
   reasoner is constructed (:2140); `tm_recompute` bypasses it entirely
   (:4204). Even if reached, the prompt (:1966) contains no claim values.
3. **Other heuristic failure modes**: hardcoded `"has title"` semantics table
   (:780) AND a second inline has-title override (:2134-2136);
   `text.contains(claim_id)` LLM-output fallback (:2071); stale model
   `claude-3-5-sonnet` (:1920); `context: None` hardcoded (:3910) making
   SplitByContext degenerate; `predicate_conflict_no_semantics` outbox event
   (:2116) with zero consumers.

Upstream, entity resolution at promotion
(`crates/pipeline/src/scheduler/inline_promotion.rs:501-516`) reuses an entity
on exact lowercased-name identity-key match (`find_by_identity_key` LIMIT 1,
entity_repository.rs:448), no type check. The wired `AgenticDisambiguator`
(resolution/mod.rs:235, impl :544) is dead code. The summarizer is invoked
with model `"default"` (scheduler/evidence_work_kinds.rs:872-874) which fails
and silently degrades to prefix truncation; the struct default
(summarizer/mod.rs:46-49) is also stale.

## Goals

1. Conflict detection compares **values**; identical values corroborate
   (evidence merges) instead of fake-conflicting; divergent objects on
   single-valued (or unknown-cardinality) predicates DO conflict.
2. Real conflicts are adjudicated by an **LLM that sees the actual values**
   via meerkat structured output. Deterministic policies are the fallback for
   keyless deployments, not the primary path. Verdicts are **durable**:
   memoized per claim-set and backed by persisted learned semantics, so
   deterministic recomputes never resurrect superseded values.
3. Predicates without configured semantics get **LLM-proposed semantics**
   persisted with distinct provenance status, revisable by later
   adjudications, never overwriting operator-configured values.
4. Entity reuse at promotion is guarded (normalized-type compatibility with
   `topic` as wildcard) with LLM disambiguation for ambiguous cases.
5. Proven by e2e tests: deterministic tier (CI, injected fakes, no env
   dependence) + real-LLM tier (`#[ignore]`, ANTHROPIC_API_KEY).
6. A **prompt calibration harness** with per-field scoring over labeled golden
   cases.

## Non-goals (follow-ups)

Wiring AgenticTriage; LLM predicate hygiene / entity-type sweep / entity label
hygiene / freshness scoring; event slot *keys* (event conflict *resolution* IS
in scope); embedding fake-provider fallback hardening; MCP/API merge
heuristics; cross-security-hash conflicts (ABAC-correct blind spot, now
documented).

## Design

### D0. TmContext  -  the injection seam (no env reads in library code)

```rust
pub struct TmContext {
    pub adjudicator: Option<Arc<dyn SlotAdjudicator>>,
    pub config: AdjudicationConfig, // model, max_tokens, wall-clock budget
}
```

Constructed ONCE in `bin/elephant-pipeline` from env
(`ANTHROPIC_API_KEY` presence + `PIPELINE_TM_MODEL`, default
`claude-sonnet-4-5`), threaded as a parameter through
`execute_tm_resolve_slot`, `resolve_conflict_group_with_storage`,
`execute_tm_recompute`, and the worker arms (main.rs TM_RESOLVE_SLOT /
TM_RECOMPUTE / TM worker loop). Availability = `ctx.adjudicator.is_some()`.
Tests pass `None` or fakes explicitly  -  never via env. The existing
`SlotReasoner` trait is renamed `SlotAdjudicator` (same shape); `FakeReasoner`
becomes the injectable test double. Binary-side constructor gets
key-present/key-absent unit tests without network.

### D1. Claim values flow into TM

`ClaimWithMetadata` (NOT serialized in payloads  -  verified) gains:

```rust
pub value: ClaimValue,

pub enum ClaimValue {
    Literal(Literal),
    EntityRef { id: String, display: String },
    EventClaim { title: String, time_start: Option<DateTime<Utc>>,
                 participants: Vec<String>, status: String },
}
```

Populated in `build_claim_metadata` / `build_event_claim_metadata`; rel object
displays via new `TruthMaintenanceStorage::get_entity` (fetch failure degrades
to id-only display, never errors). All test constructors of ClaimWithMetadata
updated (mechanical, many sites).

### D2. Value-aware detection + corroboration

After loading slot claims, partition into **value groups** by exact equality  -
Vec-scan with `PartialEq` (Literal has no Eq/Hash: Float/Json). NaN or
Json-normalization mismatches yield distinct groups, which merely routes to
adjudication (documented, acceptable).

- **1 value group, N>1 claims → corroboration.** DirectCommit with new
  `ResolutionResult::CorroboratedWinner { winner_id, merged_claim_ids,
  confidence, rationale }`. Winner = highest confidence (tie: earliest
  created_at  -  harmless: values identical; evidence of all members merges).
  Any existing unresolved group for the slot is resolved as corroborated.
- **>1 value groups → real conflict** → conflict group (as today).

**Commit semantics for corroboration (and for `equivalent_groups` in ALL LLM
verdicts):** merge evidence refs (deduped by assertion id + span) into the
group winner, then **soft-delete** the duplicate rows (deleted_at, exactly
like the existing maintenance dedupe sweep)  -  NOT Superseded, NO validity
windows (a corroborating duplicate never "stopped being true"). Detection then
converges (soft-deleted rows leave the slot). Winner keeps/opens its validity
window; if a previous fake-conflict commit had wrongly closed the winner's
window, the corroboration commit re-opens it (self-heal, D11).

`CorroboratedWinner` IS serialized into tm_commit payloads
(CommitTruthPayload, main.rs:5353/:5464): additive variant, fine for the
single-supervisor deployment; documented constraint  -  drain TM queues before
any downgrade.

### D3. Cross-object rel conflicts (cardinality)  -  env-independent

New slot type `"rel_set"`, key = `subject::__rel_set__::predicate::hash`
(4 parts; sentinel segment cannot collide with attribute keys, whose part[1]
is a predicate string; parsed only under slot_type == "rel_set"). Unit test
asserts non-collision with attribute/rel keys for the same subject+predicate.

During rel event processing, detection loads all rels for
(subject, predicate) via new `find_rels_for_subject_predicate`
(post-filtered by `compute_security_hash` in Rust, like existing callers). If
>1 distinct objects AND the predicate is not event-like:

- `Cardinality::One` configured → upsert rel_set group.
- semantics missing → upsert rel_set group (ALWAYS  -  no env gating; detection
  topology is deterministic and replay-stable).
- `Cardinality::Many` configured → no group.

Resolution decides:
- adjudicator present → LLM (D4), which MUST return `proposed_semantics` when
  the predicate has none (D5).
- no adjudicator → resolve as coexist (`ParallelValues`, all approved),
  group marked resolved. Converges; no supersession ever happens without
  either configured semantics or an LLM verdict; keyless behavior differs
  from today only in bookkeeping (a resolved group + a subject-scoped truth
  slot recording coexistence).

Conflict-group lookups for detection/commit gain a slot_type-filtered variant
(`find_unresolved_by_slot_key_and_type`; old method kept) as defense in depth
against key ambiguity.

**Migration 016**: widen `ASSERT $value INSIDE [...]` on `truth_slot.slot_type`
and `conflict_group.slot_type` (001_init.surql:589/:633) to
`['rel','attribute','event','rel_set']`  -  also fixing the latent 'event'
violation on SCHEMAFULL remote deployments; add new optional conflict_group
fields (D4 memoization). Note in PR: remote DBs must migrate before deploy.

### D4. Reachable LLM adjudication

Flow in `execute_tm_resolve_slot` (also the path for tm_recompute  -  D7):

```
claims = load with values
if 1 value group                 -> CorroboratedWinner (no LLM)
else if ctx.adjudicator.is_some() -> adjudicate (memoized)
else                              -> deterministic policy (today's fallback)
```

The dead gate (`should_use_reasoner`, high_conflict/high_salience thresholds),
the `text.contains` fallback, both has-title hardcodes, and the stale model
string are deleted.

**Adjudicator impl** (replaces AgenticSlotReasoner internals): meerkat
`AgentFactory` + `OutputSchema::new(schema).strict()` +
`structured_output_retries(2)` (identity_sweep.rs:1158-1190 pattern, but as a
**pub** function taking a plain input struct  -  claims with values, semantics,
subject/predicate displays  -  no storage handle, so the calibration harness
feeds pure data). Prompt in `crates/pipeline/prompts/tm_adjudication.txt`
(placeholder templating). Prompt contains: subject display, predicate,
per-claim rendered value, confidence, evidence count, source timestamps +
time basis, semantics if known, and for events: title/time/participants/
status. Malformed output after retries → deterministic fallback + warn (no
substring rescue). Every returned claim id validated against the group.

**Verdict schema:**

```json
{
  "decision": "corroborate | single_winner | coexist | supersede | needs_review",
  "winner_id": "...",
  "equivalent_groups": [["id1","id2"],["id3"]],
  "superseded": [{"claim_id": "...", "valid_until": "ISO-8601 optional"}],
  "winner_valid_from": "ISO-8601 optional",
  "proposed_semantics": {
    "cardinality": "one|many",
    "temporal_model": "persistent|state_over_time|event_like",
    "conflict_policy": "allow_parallel|supersede_by_recency|require_more_evidence"
  },
  "rationale": "..."
}
```

- `proposed_semantics` REQUIRED when the predicate has no registered
  semantics (schema-enforced); `split_by_context` is NOT proposable (context
  is unpopulated today).
- **Temporal fidelity**: `superseded[].valid_until` / `winner_valid_from`
  carry dates the LLM read in the values. tm_commit prefers verdict dates over
  `claim_timestamp_for_semantics` (which falls back to ingest time); the
  timestamp heuristic remains only as fallback; basis recorded in
  temporal_audit.
- Mapping: corroborate → CorroboratedWinner; single_winner/supersede →
  SingleWinner + windows (equivalent_groups first merge into their group
  winners per D2); coexist → ParallelValues (ALL approved); needs_review →
  NeedsReview.
- `CommitInput` gains `claim_set_hash` (sha256 of sorted claim ids; optional,
  serde-default)  -  tm_commit re-loads claims and **no-ops with a warn** if the
  live set changed (stale-adjudication guard) and `adjudicated: bool`
  (default false).
- **Memoization**: ConflictGroup gains optional `adjudicated_claim_set_hash` +
  `adjudicated_at` (migration 016). Resolution skips the LLM when the current
  claim-set hash equals the stored one (covers the reopen-on-every-touch cost
  loop; needs_review verdicts also memoize  -  retried only when the claim set
  changes).

**Events** flow through the same path (`slot_type == "event"` early-return
removed; `resolve_event_group` = deterministic fallback). Commit side
(`execute_tm_commit_events`) is fixed to stop fighting verdicts:
- ParallelValues approves ALL ranked ids (today: only first  -  a coexist
  verdict currently keeps one event).
- New CorroboratedWinner arm (merge + soft-delete duplicates).
- `event_reject_floor` applies ONLY when `adjudicated == false`.

### D5. LLM-proposed predicate semantics  -  durable, revisable

`upsert_predicate_semantics(space, predicate_id, semantics, provenance)`:
- Persists with a distinguishing `semantics_status` (LLM-proposed ≠
  operator-Configured; reuse the existing status enum or extend it).
- NEVER overwrites Configured semantics.
- Later adjudications may REVISE non-Configured semantics (e.g. a coexist
  verdict on a One-cardinality predicate downgrades it to Many); both
  directions audit-logged.
- Unspecified PredicateSemantics fields take Defaults (recency_basis default
  order documented).
- Emits the standard `predicate_registry` outbox event → existing
  `mark_slots_stale_by_predicate` machinery → tm_recompute converges old
  slots under the learned semantics. This closes the
  `predicate_conflict_no_semantics` dead-event gap.

### D6. Entity-reuse guard at promotion

- `find_by_identity_key_all` (no LIMIT 1) added alongside the existing method.
- Incoming extractor type passes through `normalize_entity_type` FIRST.
  Compatibility over the 8-value vocabulary: `topic` (the unknown bucket) is
  wildcard-compatible with everything; only conflicts among the 7 specific
  types are incompatible. The entity_id fast-path (:476-497) stays exempt
  (trusted extraction).
- Compatible single match → reuse (today). Incompatible or multiple distinct
  compatible matches → LLM disambiguation via a NEW slim promotion-facing
  entrypoint (mention text + incoming type + doc snippet + candidates'
  name/type/summary → reuse-X | create-new), meerkat structured output;
  budget/timeout like the adjudicator. NOT the old DisambiguationContext
  (whose prompt renders fabricated feature scores).
- No LLM: incompatible → create new entity (safer than false merge);
  multiple compatible → first match + warn (today's behavior).
- Deterministic test: untyped-then-typed sequence ("Acme" topic → "Acme" org
  reuses, does not duplicate).

### D7. tm_recompute  -  no inline LLM (lease safety)

Work-item lease is 300s with no renewal (work_queue.rs:26); recompute loops up
to 100 slots. Therefore: recompute inline-commits ONLY single-value-group
slots (deterministic, fast). Multi-value-group stale slots get their conflict
group upserted and returned in `RecomputeResult::pending_conflict_groups`; the
bin arm enqueues `tm_resolve_slot` per group (existing machinery, one LLM call
per item, bounded by AdjudicationConfig wall-clock ≤ 120s < lease).

### D8. Summarizer model fix

Fix call site `scheduler/evidence_work_kinds.rs:872-874` (the `"default"`
string) AND `SummarizationOptions::default()`: model from
`PIPELINE_SUMMARIZER_MODEL`, default `claude-sonnet-4-5`. Failure still falls
back to truncation but logs `warn!` with the error.

### D9. Tests

**Deterministic tier (CI-safe: every test passes adjudicator None or a fake;
zero env reads; zero network):**
- Value-group partitioning (incl. Literal PartialEq quirks: NaN, Json).
- Corroboration: identical attr values from 2 sources → direct commit, no
  conflict group, evidence merged, duplicate soft-deleted, NO windows.
- Attribute value conflict → group; FakeAdjudicator verdict of each decision
  kind → committed outcome (incl. verdict-date windows, equivalent-group
  merge, coexist approves all).
- rel_set: configured One + 2 objects → group; missing semantics + 2 objects →
  group; keyless resolution → coexist + resolved; Many → no group; event-like
  predicate skipped; key non-collision test.
- Memoization: unchanged claim-set hash skips adjudicator (fake counts calls);
  claim_set_hash mismatch → tm_commit no-op.
- tm_recompute: single-group inline; multi-group returns pending; fake
  adjudication through enqueued path.
- Promotion guard: type compatibility matrix (topic wildcard), untyped→typed,
  fake disambiguator reuse/create paths.
- Full-chain (template main.rs:1216): two sources corroborate; two conflicting
  values + FakeAdjudicator → winner committed.
- **Intentionally rewritten tests** (asserted OLD behavior, enumerated for
  review): mod.rs:4535, :5506, :6525, :6697 (identical-value "conflicts" →
  now corroboration), :6451 (has-title table), :4909/:4951 (dead-gate
  assertions). s5_14 e2e_002 pinned to adjudicator None; its
  approved/superseded asserts re-verified against CorroboratedWinner
  semantics. Survivors: s4_4, m9 (CRUD-only), main.rs:3323/:3425.

**Real-LLM tier (`#[ignore]`, ANTHROPIC_API_KEY):**
- `s5_19_truth_adjudication.rs`: docs A ("Alice works at ExampleCo since 2020, age
  35") and B ("Alice, 35, left ExampleCo and joined ExampleLabs in 2024") through the
  s5_13-style extractor, then an EXPLICIT outbox-driven TM phase
  (get_after_seq → execute_tm_detect_conflicts_from_outbox_events →
  direct commits → resolve+commit per group; pattern main.rs:1338-1361  -
  drive_pipeline TM arms alone are unreachable, nothing enqueues TM items
  in-process). Asserts via `TruthSlotRepository::find_by_subject_entity` (+
  predicate filter; no slot-key computation  -  compute_security_hash is
  private): age corroborated (one approved claim, merged evidence, no
  disputed), works_at adjudicated (ExampleLabs current; ExampleCo superseded with a
  2024 window end, proving verdict dates beat ingest dates), audit log carries
  the adjudicator rationale, learned semantics persisted for `works_at`.
- Direct adjudicator tests: contradiction, formatting equivalence
  ("NYC"/"New York City"), multi-valued coexistence ("has hobby").

### D10. Prompt calibration harness

New workspace member `tests/calibration` (package `elephant-calibration`,
member + NOT default-member, mirroring tests/e2e wiring incl.
`elephant-pipeline` dep with `meerkat-extraction` feature; clippy
pedantic/nursery compliant).

- Case schema: `{id, description, subject, predicate, semantics?, claims:
  [{id, value, confidence, evidence_count, timestamp, time_basis}],
  expected: {mapped_variant, winner_id?, superseded_ids? (set equality),
  equivalence_partition? (partition equality), cardinality?},
  acceptable_alternatives: [], tags}`.
- Scoring per field: hard = mapped variant + winner_id + superseded set;
  soft = equivalence partition + proposed cardinality. Separate accuracy
  floors (assert only when a key is present).
- Suites: `cases/tm_adjudication.json` (~20 cases: corroborate, contradict,
  formatting-equivalent, temporal supersession with explicit dates,
  multi-valued coexist, needs-review ambiguity),
  `cases/entity_disambiguation.json` (~10 cases, pure-data via the slim D6
  entrypoint).
- Runner calls the pub production entrypoints with pure-data inputs; writes
  `target/calibration/<suite>/results.json` (model, prompt-file sha, per-case
  verdict + rationale) for prompt-version diffing; prints a per-case table.
- `#[ignore]`d; **first line guards on missing ANTHROPIC_API_KEY with
  eprintln + early Ok** so `cargo integration` / `cargo test-e2e` (which run
  --ignored workspace-wide) pass keyless.

### D11. Self-healing of old data

No data migration of existing slots. Old fake-corroboration victims (wrongly
Superseded + windowed duplicates) self-heal when their slot is next touched or
recomputed: the corroboration path merges them and re-opens the winner's
window. Learned-semantics upserts mark slots stale via the predicate outbox
event. PR notes an optional ops step: trigger a recompute sweep
(mark_all_slots_stale) after deploy to accelerate convergence.

### D12. Docs & verification

- `docs/active-architecture.md`: TM section rewritten (value-aware detection,
  adjudication flow, rel_set, semantics learning, TmContext env vars,
  memoization).
- `docs/concepts/truth-maintenance.mdx`: aligned (it currently promises
  value-aware behavior the code lacks  -  after this change it's true).
- `scripts/verify_active_surface.sh` kept green. No new work kinds; the DAG
  is unchanged.

## Invariants

- Work-kind DAG unchanged. Detection deterministic & replay-stable (no env
  dependence). Deterministic conflict-group ids (now also over rel_set keys).
- space_id isolation + security_hash post-filtering on every new query path.
  Cross-security-hash contradictions never join a group (ABAC-correct,
  documented blind spot).
- Keyless deployments: no supersession without configured semantics or an LLM
  verdict; behavior deltas vs today are limited to (a) coexist bookkeeping for
  rel_set groups, (b) corroboration merge instead of fake supersession, (c)
  promotion type guard preferring new-entity over false-merge  -  all
  deliberate and listed in the PR.
- Serde: new ResolutionResult variant + optional CommitInput/ConflictGroup
  fields are additive; old payloads deserialize (defaults). Forward
  incompatibility (new payload, old binary) accepted for the single-supervisor
  deployment; drain TM queues before downgrade.
- SurrealDB boundary rules respected for all new fields/proxies; migration
  016 covers schema ASSERT widening + new optional fields.

## Work packages

- WP1 types+storage+migration: ClaimValue, CorroboratedWinner, CommitInput
  fields, ConflictGroup memo fields, trait methods (get_entity,
  find_rels_for_subject_predicate, find_unresolved_by_slot_key_and_type,
  upsert_predicate_semantics, find_by_identity_key_all), repo impls (both
  backends), migration 016.
- WP2 TM core: TmContext threading, value groups, rel_set detection,
  corroboration commit (merge + soft-delete + window rules), event commit
  fixes, memoization, recompute enqueue split, heuristic deletions.
- WP3 adjudicator: prompt file, pub pure-data entrypoint, meerkat structured
  output, verdict mapping incl. temporal fields, semantics persistence.
- WP4 promotion guard + slim disambiguator.
- WP5 tests (deterministic rewrites + new; s5_19 real-LLM).
- WP6 calibration harness.
- WP7 summarizer fix.
- WP8 docs.
