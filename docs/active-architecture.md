# Active Architecture (Code-Truth)

This document is the canonical source of truth for what Elephant runs today.
If this file conflicts with older design notes or historical docs, this file wins.

## Runtime Topology

Elephant now has a managed runtime layer.

Primary runtime:
- `elephant serve`: HTTP + MCP server plus managed runtime owner.
- storage backend: `surreal-db` (default, persistent) or embedded `elephant-db`.
- in configured write-capable modes, `elephant serve` also starts the background
  pipeline worker loops in-process through the shared runtime manager.

Advanced runtime:
- `elephant worker`: reusable worker runtime used by the managed runtime and
  available as a standalone advanced surface for SurrealDB-backed deployments.
- `elephant serve --server-only`: HTTP/MCP server without managed workers.

Managed runtime modes:
- `bootstrap`: no `.elephant/elephant_config.toml` yet; only runtime status and
  initialization/configuration surfaces are exposed.
- `local_memory`: project-local `elephant-db` plus managed workers.
- `shared_memory`: external `surreal-db` plus managed workers.
- `read_only`: query surfaces only; mutation/ingest tools are hidden.
- `maintenance`: admin/maintenance writes without semantic ingest unless the
  runtime is fully ready.

Pipeline loops (when workers are running):
1. Work queue loop (claims and executes work items).
2. Truth maintenance outbox loop (turns outbox events into TM work).
3. Embed loop (node embedding work).
4. Maintenance loop (freshness and scheduled sweeps).

Config source of truth:
- `.elephant/elephant_config.toml` at the project root (nearest `.git`, else cwd)
- env-only startup remains a compatibility fallback when no config file exists

## Entity Authorization Boundaries

HTTP claim conversion preserves the difference between an absent
`subject_allowlist` and an explicitly empty list. An empty list never becomes
unrestricted; subject-bearing records remain denied even with wildcard scopes.
The policy engine's explicit subjectless handling exception is unchanged.

REST entity replacement, upsert, and batch writes and MCP entity update/batch
writes require read and write authorization for existing and proposed records.
These ordinary interfaces cannot change any field of an existing `Security`
object. There is no ordinary declassification scope. Security-preserving storage
methods compare the current security and replace the record atomically on both
backends. Ordinary replacements additionally preserve identity, subjects and raw
subject completeness, and compare the authorized timestamp, lifecycle and full
envelope at that same commit, including restoration. Stale edits cannot revert
a newer classification or subject change. Trusted internal maintenance retains the low-level update API
for intentional security changes.

## Active Source, Extraction, and Commit Path

Active path:

`ingest source -> triage_source -> ingest_metadata -> validate_artifacts -> commit_artifacts -> upgrade_ingest_mode -> render_doc -> build_doc_tree -> plan_extraction -> extract_assertions -> validate_artifacts -> commit_artifacts -> outbox event -> truth maintenance`

Key behavior:
- Source-first ingestion is the canonical lifecycle. Doc-ingest entrypoints are
  convenience wrappers that create a `Manual` source first.
- File and URL PDF ingestion extracts plain text in-process with `pdf-inspector`.
  Its optional Python and OCR features are disabled. The extracted text enters
  the same source-first lifecycle as other text documents. A pinned compatibility
  patch preserves form text, existing OCR text layers, and plain text output; see
  `vendor/pdf-inspector/ELEPHANT_PATCH.md` for its scope and upgrade tests.
- Media-backed docs follow the same active path. Image/video inputs are
  attached to the source/doc lineage, `render_doc` produces the canonical text
  projection, and extraction remains downstream of `build_doc_tree`.
- `extract_doc` remains an explicit maintenance/manual replay helper for
  already-ingested docs. It is not a competing ingest lifecycle and must stay
  out of the normal source-first path.
- Original media payloads and external media references belong to source/doc
  lineage and revision manifests, not to alternate searchable content fields.
- `doc.text` is the only canonical searchable projection for media-backed docs.
  Attachment metadata, time ranges, and other multimodal provenance remain
  supplementary manifest data and must not become a competing canonical text
  path.
- Extraction intent remains metadata/skill-driven for media-backed docs just as
  it is for text docs. Modality may change the tool/input blocks available to
  the renderer or extractor, but it must not become a hardcoded business-logic
  substitute for `extraction_skill`, `extraction_purpose`, or
  `extraction_domain`.
- Semantic ingest is only exposed when the runtime reports
  `semantic_ingest_ready=true` (storage live, workers running, real extractor ready).
- `ingest_metadata` stages normalized `source_metadata` artifacts; the source
  lifecycle only advances to `metadata_ready` after `commit_artifacts`.
- `upgrade_ingest_mode` is the depth transition after metadata commit:
  metadata mode stops there, retrieval mode materializes evidence, and graph
  mode continues through extraction + artifact commit.
- `extract_assertions` stages `knowledge_candidates` artifacts; it does not
  commit canonical knowledge directly.
- `commit_artifacts` is the active boundary between staged interpretation and
  committed assertions/graph/event rows via typed `commit_bundle` payloads.
- `resolution_patch_set` bundles now commit through the same
  `validate_artifacts -> commit_artifacts` boundary; the older
  `validate_resolution_patches -> commit_resolution_patches` path remains only
  as a compatibility dispatch surface.
- `extract_assertions` requires a real agentic/LLM extractor. The old heuristic
  fallback is no longer part of the active runtime; missing extractor readiness
  is a hard runtime/work-item error.
- Entity reuse at promotion is type-guarded. Identity-key lookups fetch all
  matches (not first-match), the incoming extractor type is normalized first,
  and reuse requires normalized-type compatibility (`topic` is the unknown
  bucket and acts as a wildcard; a `topic` entity upgrades to the specific
  incoming type on reuse). Ambiguous or type-incompatible matches route to an
  LLM disambiguator (prompt `crates/pipeline/prompts/promotion_disambiguation.txt`,
  model from `PIPELINE_PROMOTION_DISAMBIGUATION_MODEL`, default
  `claude-sonnet-4-5`, active when `ANTHROPIC_API_KEY` is set). Without an
  adjudicating LLM the guard never false-merges: incompatible types create a
  new entity; multiple compatible matches reuse the earliest-created match
  with a warning.
- Doc-tree section summaries use a real summarizer model
  (`PIPELINE_SUMMARIZER_MODEL`, default `claude-sonnet-4-5`); summarization
  failure falls back to truncation with a logged warning.
- Legacy `commit_assertions -> resolve_assertions -> synthesize_rels_attrs`
  remains removed from the active worker.

## Active Living Wiki

The active Living Wiki is a graph-native derived read model, not a source of
truth. The entity compiler reads the root entity, relationships, attributes,
events, truth slots, assertion evidence, counterpart labels, and claim
freshness. The source compiler reads lifecycle state, attached documents, and
immutable document revisions. Both publish a stable `knowledge_view` pointer
plus immutable `knowledge_view_revision` receipts.

Publication is security-envelope exact. The root entity security object is
fingerprinted into the view key, and only records with the same complete
security envelope contribute to the page. Every revision records its complete
dependency set, citations, compiler version, dependency hash, freshness state,
and superseded revision.

Projection authorization retains the union of the subjects of every contributing
record, including relationship endpoints and evidence. Incomplete subject
metadata stays incomplete. Source projections also require `read:doc:raw`
because they contain immutable document excerpts. Revisions created before
this authorization contract must be recompiled before any read; stale fallback
cannot expose those revisions. Organizer metadata retains the complete input subject
scope, while article evidence selection still uses the typed article roots.

`create_wiki_from_graph` and `compile_wiki_article` require an explicit `manage:wiki`
scope and an unrestricted subject scope. Existing wildcard operation scopes
do not grant this service authority. This is an explicit delegation to the
space-wide wiki service, including all security envelopes in that space. It
is separate from ordinary entity read/write access. Recurring jobs carry a
required space-bound service grant; legacy jobs without it fail closed and
must be recreated by an authorized manager. A durable per-space pointer owns
the current grant generation; historical registration status alone confers no
authority. `stop_managed_wiki` clears that pointer and cancels queued work.
Enqueue and revocation recheck committed authority to close concurrent database
snapshot races. Revocation fences graph-change and successor scheduling,
including old completed ancestors. A publication already in progress may finish, but
cannot schedule more work. Explicitly creating a new managed wiki replaces the
prior grant generation. Development `SKIP_AUTH` retains its explicit bypass
behavior.

Claim-bearing prose is represented as statement records. Each statement has
local citations, a derivation mode, and a support-verification mode. Factual
Markdown must be exactly rendered from this statement ledger. Publication
fails for uncited statements, unresolved citations, prose outside the ledger,
or verbatim quotations without immutable evidence spans. Deterministic entity
and graph compilers build bounded inspector context. The separate wiki
editorial compiler uses Meerkat structured output to write semantic prose,
runs an independent statement-level entailment and subject-relevance
verification call, and only publishes an `article` projection when its
governed context and freshness receipt pass the publication gate. A supported
fact that is off-topic for the named article is rejected rather than retained
as filler.

Outbox record changes mark dependent views dirty. Reads can allow stale data,
prefer a refresh with authorized stale fallback, or require refresh. Forced
maintenance rebuilds always publish a new immutable revision. Event,
relationship, and attribute promotion creates or updates deterministic
claim-freshness rows used by the compiler.

Active MCP tools include `get_knowledge_view`, `rebuild_knowledge_view`,
`materialize_knowledge_views`, `compile_wiki_article`,
`create_wiki_from_graph`, typed graph-view definition reads/writes, and
the source-watch configuration, inspection, and refresh-enqueue surface. The
bounded materializer scans resumable ID-ordered pages of existing entities,
sources, and definitions, applies record-level ABAC, isolates per-root
failures, and writes only derived revisions. Active scope includes
entity-rooted, source-rooted, topic, seeded-community, and saved-query pages.
Publication maintains a current reverse dependency index, so outbox
invalidation resolves affected views without scanning every view or immutable
revision. Selector table triggers cover newly created records that were absent
from the previous revision.

The graph-to-wiki workflow scans every entity under its space-wide service grant plus relationships,
assertions, attributes, events, truth slots, and predicate governance. It
partitions every eligible entity into bounded graph-neighborhood prompts, so
the context bound does not discard a global tail before LLM curation. An
exhaustive article budget equal to the eligible population is used when no
explicit cap is supplied, up to 2,000 pages. The planner must account for every
entity exactly once. Only possible identity duplicates may be synthesized into
one multi-entity page, and the complete plan must retain at least 70 percent of
eligible subjects as articles. A separate LLM information architect sees the
complete corpus, infers its purpose, selects a small global featured set, and
assigns every article exactly once to semantic reader sections. `/wiki` shows
that featured hierarchy and section counts; each section remains browsable in
full without placing hundreds of reference pages on the front page. Navigation
is derived metadata and never becomes a graph fact or source.

Elephant fills omitted related-article links from explicitly governed graph
edges and resolved source-evidence co-mentions, then uses separate writer and
verifier calls for every article. Writer contexts include governed current
attributes, approved first-class events, truth-slot uncertainty, canonical
relationships, and hash-validated source passages. Only explicitly configured
predicate semantics may contribute graph claims. Exact and likely duplicate
people are supplied as identity-adjudication clusters instead of being
withheld. The independent verifier checks both sentence entailment and the
whole page for useful synthesis, coherence, temporal framing, and omitted major
supported topics. The reconciliation receipt reports claim rows withheld by
predicate governance.

A successful published creation enqueues the durable
`reconcile_living_wiki` work kind. Periodic runs scan and fingerprint the full
curation input but spend no planner, organizer, writer, or verifier calls when
the graph is unchanged. Semantic outbox changes debounce a replacement run,
clear that cache decision, and cancel older queued periodic work without
interrupting a running reconciliation. Wiki work is serialized per space;
article compilation uses bounded concurrency inside the one run. Successful
runs hide planner-owned pages no longer retained, preserve immutable history,
and schedule the successor using the completed work item as its idempotency
scope, avoiding collisions with a cancelled periodic item in the same time
bucket. Failed articles leave the fingerprint uncached so the next run retries
instead of accepting a partial wiki as current. The
authenticated `/wiki` browser renders the LLM-organized sections over verified
English revisions, and `elephant demo-wiki` remains the small isolated example.
Knowledge-view invalidation has its own event watcher and does not depend on
the truth-maintenance worker being enabled. Direct entity API mutations also
publish semantic outbox records, so creates, updates, deletes, restores,
upserts, and batches participate in the same automatic maintenance path.

## Active Pipeline Work Kinds

These work kinds are dispatched by `bin/elephant-pipeline/src/main.rs` today:

- `ingest_source_stub`
- `triage_source`
- `ingest_metadata`
- `upgrade_ingest_mode`
- `render_doc`
- `plan_extraction`
- `extract_assertions`
- `validate_artifacts`
- `commit_artifacts`
- `validate_resolution_patches`
- `commit_resolution_patches`
- `execute_reprocess_job`
- `tm_detect_conflicts`
- `tm_resolve_slot`
- `tm_commit`
- `tm_recompute`
- `freshness_sweep`
- `reconcile_living_wiki`

## Active Truth Maintenance

The TM work-kind chain is unchanged
(`tm_detect_conflicts -> tm_resolve_slot -> tm_commit`, plus `tm_recompute`
for stale slots). Behavior inside the chain is value-aware and
adjudicator-first.

Detection (value-aware, deterministic, replay-stable):
- Slot claims are partitioned into value groups by exact value equality.
- One value group with multiple claims is corroboration, not a conflict:
  detection direct-commits a `CorroboratedWinner`, evidence merges into the
  winner, duplicate rows are soft-deleted, and no validity window is closed.
- More than one value group is a real conflict and produces a conflict group.
- Cross-object rel conflicts use the `rel_set` slot type with key
  `subject::__rel_set__::predicate::security_hash`. A rel_set group is
  upserted when a subject+predicate has more than one distinct object and the
  predicate's cardinality is `One` or its semantics are unknown. Configured
  `Many` cardinality and event-like predicates never produce rel_set groups.

Resolution (LLM adjudication is the primary path for real conflicts):
- `TmContext` is the injection seam, constructed once in
  `bin/elephant-pipeline` and threaded through the TM entry points; library
  code reads no env vars. The adjudicator exists when `ANTHROPIC_API_KEY` is
  set; model from `PIPELINE_TM_MODEL` (default `claude-sonnet-4-5`); prompt
  `crates/pipeline/prompts/tm_adjudication.txt`; meerkat structured output
  with a strict schema and retries.
- Verdict decisions: `corroborate`, `single_winner`, `coexist`, `supersede`
  (with LLM-extracted validity dates preferred over ingest-time fallbacks),
  `needs_review`, plus `equivalent_groups` whose members merge into their
  group winner like corroboration.
- When the predicate has no registered semantics the verdict must include
  proposed semantics, persisted to the predicate registry with
  `semantics_status = llm_proposed`. LLM-proposed semantics never overwrite
  operator-`configured` semantics, are revisable by later adjudications, and
  emit the standard predicate-registry outbox event so dependent slots go
  stale and recompute.
- Without an adjudicator (keyless deployments), deterministic conflict
  policies (`AllowParallel`, `SupersedeByRecency`, `SplitByContext`,
  `RequireMoreEvidence`) remain the fallback, and rel_set groups resolve as
  coexist. Keyless deployments never supersede a value without configured
  semantics.
- The old reasoner gate, the hardcoded "has title" semantics, and the
  substring-parse LLM output fallback are removed.

Cost and safety rails:
- Verdicts are memoized per claim-set hash on the conflict group
  (`adjudicated_claim_set_hash` / `adjudicated_at`); an unchanged claim set
  never re-invokes the LLM.
- `tm_commit` no-ops with a warning when the live claim set no longer matches
  the resolution's claim-set hash (stale-adjudication guard).
- `tm_recompute` never calls the LLM inline: single-value-group stale slots
  commit deterministically in the recompute loop; multi-value-group stale
  slots enqueue `tm_resolve_slot` items, keeping each work item inside the
  300s lease.
- Event conflicts flow through the same adjudicator. The event commit path
  approves all coexisting ranked values; the event reject floor applies only
  to non-adjudicated resolutions.

Deliberate blind spots:
- Claims in different security envelopes (different `security_hash`) never
  share a slot or conflict group, so cross-envelope contradictions are
  invisible to TM. This is ABAC-correct isolation, not a bug.
- Re-corroboration of a superseded value does not reopen adjudication while
  the conflict group's claim set is unchanged (memoization by design).

Schema: migration `016_tm_adjudication` widens `slot_type` on `truth_slot`
and `conflict_group` to `['rel','attribute','event','rel_set']` and adds the
adjudication memo fields plus the `llm_proposed` semantics status.

## Non-Active Surfaces (Separated by Intent)

### Legacy (Removed)

Removed from runtime and code surfaces. Listed here only for migration/history context:

- `commit_assertions`
- `resolve_assertions`
- `synthesize_rels_attrs`
- `extract_match_keys`
- `resolution_sweep`
- `resolve_contradiction`

Code namespace:
- none (removed)

### Planned (Not Yet Implemented)

Scaffolded for future implementation and intentionally non-active:

- `reembed`
- `rebuild_indexes`
- `snapshot_export`

Code namespaces:
- `scheduler::work_kinds::experimental`

See `docs/non-active-catalog.md` for policy and ownership rules.

## Authentication Context

HTTP and MCP use `elephant-auth` as the single legacy bearer-token verifier.
Authenticated requests retain an opaque verified caller context, including
issuer, audience and expiry, alongside the existing public Principal/ABAC
projections. Those projections remain policy input, not authentication proof.
Explicit skip-auth and trusted in-process embedding have no verified context.

Trusted host composition can install exact issuer/audience-to-domain mappings.
The mapping requires independently configured issuer and audience checks; no
domain is inferred from a subject string or URL. Mapped identity does not grant
Elephant clearances, live delegation or governed execution. The existing local
ABAC evaluator and subject-scope semantics remain authoritative.

The auth crate also validates a strict, versioned, request-bound exchange syntax
with required qualified identities, operation bindings and explicit time bounds.
This is authenticated claim evidence only. Existing HTTP/MCP tool routes refuse
its distinct token type; no governed resource endpoint is active. Canonical
resource generations, live grant/revocation checks, replay/entry custody,
protected result envelopes and mandatory durable evidence remain required.

## Active Data Model Notes

- `source` is the first-class intake object. `source.lifecycle_status`,
  `source.current_ingest_mode`, and `source.latest_ingest_decision` track the
  progressive ingest lifecycle.
- `work_artifact` is the active interpretation boundary:
  - `source_metadata`
  - `extraction_plan`
  - `knowledge_candidates`
  - `resolution_patch_set`
  - `commit_bundle`
- Assertions are preserved evidence records.
- Canonical graph writes are `rel` (entity->entity) and `attribute` (entity->literal).
- Truth maintenance writes `truth_slot` and `conflict_group` records. Slot
  types are `rel`, `attribute`, `event`, and `rel_set`.
- `fact` is not part of the active model.
- `doc_node` is the active structure/embedding unit. `doc_chunk` is legacy and dropped in migrations.
- `doc.text` remains the canonical searchable projection for all docs,
  including media-backed docs. When present, media provenance is carried by
  revision render manifests that map text spans back to attachments and time
  ranges. Parent document attachments are ingest inventory only and must not
  become a competing provenance source for extraction.
- Domain `event` rows are first-class committed objects even when extraction
  cannot provide precise source event time. Missing time lands as
  `time_precision = unknown`, and later evidence for the same source lineage
  refines the existing event instead of creating a duplicate.

## Active MCP Profiles

Profiles served by API:
- `/mcp` default profile (external end-user tools plus runtime status/bootstrap surfaces).
- `/mcp/agent` read-only agent profile.
- `/mcp/maintenance` agent + maintenance writes.
- `/mcp/full` full tool surface.

Current dispatcher counts are verified in CI by script:
- Full: 91 tools.
- Default: 23 tools.
- Agent: 24 tools.
- Maintenance: 43 tools.

Runtime-admin tools exposed by the managed API runtime:
- `get_runtime_status` is always available on MCP, including bootstrap mode.
- `initialize_runtime` is available in bootstrap mode to loopback callers (or
  `SKIP_AUTH` development mode).
- `get_runtime_config` and `configure_runtime` are available once configured on
  write-capable admin surfaces.

Runtime status reports:
- `configured`
- `bootstrap_mode`
- `runtime_mode`
- `storage_ready`
- `worker_ready`
- `extractor_ready`
- `semantic_ingest_ready`
- `issues`

REST bootstrap/readiness surface:
- `GET /v1/runtime/status`

## Source Files for Verification

- Pipeline dispatch: `bin/elephant-pipeline/src/main.rs`
- Scheduler work-kind declarations: `crates/pipeline/src/scheduler/*`
- MCP dispatchers: `crates/mcp/src/handlers/mod.rs`
- MCP profile constants: `crates/mcp/src/profile.rs`
- API route wiring: `bin/elephant-api/src/main.rs` and `bin/elephant-api/src/routes/*`

## Drift Enforcement

CI enforces this file and related docs with:
- `scripts/verify_active_surface.sh`
- `scripts/verify_legacy_terms.sh`

## Scheduler capacity and cold history

Scheduler selection and counts read the active `work_item` table through the
queue indexes introduced in migration 028. Runnable counts are exact aggregates,
independent of the selector's candidate limit. Embedded SurrealDB connections
have finite execution deadlines; remote database servers require their own
server-side deadline.

Migration 029 adds durable `work_item_archive` and `work_run_archive` history.
Retention preserves public IDs, idempotency lookup, retries, failed dependency
barriers, provenance, and usage metrics. Historical job and document-status
reads include archived records. Wiki authority and its current scheduling
configuration remain available to the recurring service. SurrealDB-backed
updates and restores write both hot and archive identities in their existing
transaction, so archival conflicts with a concurrent acknowledged retry or run
update. The in-process SurrealKV 0.9.3 dependency uses a pinned snapshot-version
backport; remote SurrealDB servers require their own equivalent engine repair.
This retention guarantee does not establish general serializable authorization
fences. See the [backport provenance](../vendor/surrealkv-0.9.3-backport.md).

See [Work queue capacity and retention](work-queue-capacity.md) for
query-plan constraints, operator settings, and acceptance criteria.
