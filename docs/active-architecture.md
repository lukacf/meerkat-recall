# Active Architecture (Code-Truth)

This document is the canonical source of truth for what Meerkat Recall runs today.
If this file conflicts with older design notes or historical docs, this file wins.

Atomic source acceptance retains its shared space/configuration fence under
concurrent PUTs. Its private native executor admits at most sixteen identical
transaction attempts, with capped jitter and a two-second new-retry admission
budget, solely after definite transaction-abort conflicts. Original query
deadlines, cancellation custody, immutable bindings and semantic/CAS/security
checks remain in force; generic queries and providers keep their existing retry
policies. See the [source admission contract](internal/recall-preview-source-admission-retry.md)
for exact bounds, error classification and evidence limits.

## Runtime Topology

Meerkat Recall now has a managed runtime layer.

Primary runtime:
- `meerkat-recall serve`: HTTP + MCP server plus managed runtime owner.
- storage backend: `surreal-db` (default, persistent) or embedded `elephant-db`.
- in configured write-capable modes, `meerkat-recall serve` also starts the background
  pipeline worker loops in-process through the shared runtime manager.

Advanced runtime:
- `meerkat-recall worker`: reusable worker runtime used by the managed runtime and
  available as a standalone advanced surface for SurrealDB-backed deployments.
- `meerkat-recall serve --server-only`: HTTP/MCP server without managed workers.

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

The recurring node embedding loop discovers sorted unique space IDs from stored
Docs on both backends, then resolves each space's current embedding binding before
processing its pending nodes. Soft-deleted Docs remain in that discovery scope;
node eligibility is checked downstream. SurrealDB discovery reads typed grouped
rows and reports malformed mandatory space IDs as data-integrity errors instead
of silently skipping those rows.

Config source of truth:
- `.elephant/elephant_config.toml` at the project root (nearest `.git`, else cwd)
- env-only startup remains a compatibility fallback when no config file exists

The managed HTTP server resolves data routes from the runtime manager's current
storage for each request. Explicit initialization activates those routes in the
same process, retaining JWT authentication, runtime admission, current grants and
handler policy checks. Startup validation may be remembered after success; the
router does not retain a separate cached storage authority.

Configuration replacement uses the existing durable-file implementation: create
a private temporary file in the same directory, flush and synchronize it, rename
it over the destination, then synchronize the parent directory. Failures before
publication preserve the prior file and active configuration. A completed rename
with failed directory synchronization is explicitly uncertain; a durable save
followed by failure to stop the previous workers is also not a usable activation.
Both outcomes stop new admission, withdraw readiness, drain owned workers or
report the failed drain, and require restart. The runtime does not claim rollback
or continue serving the old configuration against a newly persisted path.

The configuration facade rejects symlink destinations/parent components,
directory destinations and parent traversal. It creates missing parents
privately without changing existing parent permissions. It relies on custody of
the configuration directory against concurrent namespace replacement; this is
not a claim of immunity to a hostile process replacing parent directories.


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

## Public authorization boundaries

Derived maintenance audits inherit the affected record's trusted before/after
source union and authorization envelope. Canonical publication rejects missing
target authority, unrelated audit provenance and weaker restrictions while
retaining the complete decision's causal inputs and inverse effects. See
[maintenance audit authority](internal/recall-preview-maintenance-audit-authority.md).

`GET /v1/docs/{id}/revisions/{revision_id}` exposes one full immutable revision
only with Doc/revision raw-read authorization and the shared original completed
publication proof. Pending replacement input does not invalidate a still-published
revision; completed replacement, retraction and erasure remove its eligibility.
The lossless JSON response is bounded to 12,065,536 bytes, and a changed original
read observation is retryable unavailability.

HTTP/MCP policy projection and standing-watch admission use the same existing
scope aliases and legacy clearance rules. Raw signed/current grant intersection
precedes ingress projection; recurring authority can only narrow the original
projected ceiling with the current grant.


REST space administration uses the existing `read:space_config` and
`write:space_config` scopes with tenant and record checks. Relationship
replacement, upsert and batch mutations preserve the ordinary security envelope
and compare the authorized pre-state at the storage write. Work retry requires
`read:work_item` and `write:work_item` and retains its guard across hot/cold
reactivation.

MCP provenance resolves and authorizes the complete evidence chain before
returning references or content. REST metadata-only document projections remove
text, sidecar text and media attachments; chunk reads authorize their document,
revision and each node. Shared REST middleware consults current runtime readiness
and blocks mutations in read-only mode. Query POST routes and authorized snapshot
export are explicit exceptions.

## Active Source, Extraction, and Commit Path

Native SurrealDB source progress writes retain the original head-row nonce and
its physical/reference conflict fences. Changing only that private nonce does
not advance the canonical planning epoch: starting or retrying one source must
not invalidate every concurrently preparing source in the space. Every other
head field, including opaque future fields, remains part of the epoch comparison;
create/delete and moves between spaces still invalidate affected spaces.
Migration 044 appends this event repair to the existing receipted schema without
changing historical migration checksums. Existing stores require explicit
`meerkat-recall migrate` before read-only startup verification succeeds.

Migration 045 extends the same distinction to `canonical_read_nonce` on
`source_head`, `source_revision`, and `ingest_decision`. Source recovery and
finalizer readiness retain these physical conflict writes, but changing only
the designated nonce fields does not advance the shared canonical epoch.
Every other field, including unknown fields, source moves, creation and deletion,
still invalidates planning. The physical and reference guard events remain in
place. This prevents a pending source's readiness check from repeatedly
invalidating a peer's preparation without any semantic progress. Final
publication still rejects an original plan after real content or authority
changes; it never refreshes that plan's epoch. See the
[finalizer readiness contract](internal/recall-preview-finalizer-readiness-epoch.md).

Canonical publication still compares the original planning epoch, source and
attempt authority at commit. Preparation never refreshes its epoch after reads.
The promotion prefix check returns the same typed preparation conflict as
source retirement when that original epoch changes before group preparation;
malformed or foreign-authority prefixes remain validation failures.
Provider-free preparation conflicts use the existing dependency retry behavior;
consulted disambiguation retains the finite execution budget. The Graph progress
regression covers overlapping source pipelines and a later manual agent write
through actual queue claims, dispatch, failure recording and due-time retries,
with fixed supported extraction and a loopback embedding transport.

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
  `claude-sonnet-5-5`, active when `ANTHROPIC_API_KEY` is set). Without an
  adjudicating LLM the guard never false-merges: incompatible types create a
  new entity; multiple compatible matches reuse the earliest-created match
  with a warning.
- Doc-tree section summaries use a real summarizer model
  (`PIPELINE_SUMMARIZER_MODEL`, default `claude-sonnet-5-5`); summarization
  failure falls back to truncation with a logged warning.
- Legacy `commit_assertions -> resolve_assertions -> synthesize_rels_attrs`
  remains removed from the active worker.

MCP runtime initialization failures include a fixed storage phase and error
category, with the operating-system errno when retained at the failing I/O
boundary. This diagnostic follows the original storage-open flow and keeps its
ownership locks and recovery barriers. The actual returned error carries its
phase and any retained I/O metadata directly through initialization. No ambient
observation, error-text comparison or digest associates separate failures.
Public metadata contains fixed labels and numbers. An absent errno means the
underlying boundary did not retain one; lower driver and durability errors whose
I/O cause was already erased retain the phase with no invented errno. Paths,
connection URLs,
credentials and arbitrary error text stay out of the MCP error response.

Source validation requires the complete producer batch to pass before staging
any commit bundle. A missing or rejected candidate produces a terminal validation
error through the original work attempt's durable failure path. Rejections stay
in their artifacts; a mixed accepted/rejected batch cannot partially publish.
An empty extraction remains a validated candidate with an empty assertion
list, distinct from a missing or rejected candidate.

If validation persisted before bundle staging was interrupted, the next attempt
reuses the original validated candidate after checking its current source,
evidence and authorization again. Generated commit bundles are excluded from
candidate selection. An existing bundle must match the candidate payload, work
run and full authorization partition; its candidate ID alone is insufficient.
These checks retain the existing canonical publication transaction and fences.

Failed work attempts retain a bounded diagnostic in their custom metrics and
terminal log. It examines at most 16 nested causes and records only static
kind, stage and code values, including extraction setup, agent execution,
support verification and grounding lineage validation. Nested provider messages,
source text and credentials are not copied into this diagnostic. Retry
classification, operation authority and the existing outer error remain unchanged.

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

When an editorial catalog includes validated immutable source passages, every
synthesized article statement must cite at least one of those passages. Graph
status can supplement its source evidence but cannot replace it. Draft validation
and the final publication receipt both enforce this condition; the independent
verifier still checks that each citation supports the complete statement. A
citation added only to satisfy the shape does not establish entailment. Governed
graph-only catalogs retain their existing article path. Compiler version 19
invalidates article reuse from before this statement-level source contract.

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

Graph contexts resolve Source lineage from their definition and selected graph
inputs even when those inputs contribute no evidence-span citation. Each Source
passes the shared current-read authority, including its live and published
security envelopes. The dependency records the published immutable timestamp;
raw metadata normalization or pending replacement input cannot substitute a new
semantic version. Missing or ineligible Sources prevent preparation. Graph
compiler version 6 requires this lineage contract before reusing old revisions.

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

Meerkat Recall fills omitted related-article links from explicitly governed graph
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
English revisions, and `meerkat-recall demo-wiki` remains the small isolated example.
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
- Recomputing an attribute, relationship or relationship-set slot with no live
  claims in its exact security/subject partition clears its obsolete projection
  and closes any unresolved group for that slot. The slot becomes current with
  no values and zero confidence, under the same maintenance transaction and
  audit fences. This makes deletion maintenance quiescent without approving a
  claim from another partition or inventing a new adjudication.

Resolution (LLM adjudication is the primary path for real conflicts):
- `TmContext` is the injection seam, constructed once in
  `bin/elephant-pipeline` and threaded through the TM entry points; library
  code reads no env vars. The adjudicator exists when `ANTHROPIC_API_KEY` is
  set; model from `PIPELINE_TM_MODEL` (default `claude-sonnet-5-5`); prompt
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

Document revision reads evaluate the immutable revision's own security envelope,
subject links and unchanged subject-completeness state, in addition to its parent
document. Raw chunk reads also enforce the selected space and revision on each
node. Provenance reads authorize the entire document/revision/assertion/node
chain before returning reference metadata or excerpts. REST space creation checks
both read and write operation scopes before checking whether the space exists.

HTTP and MCP use `elephant-auth` as the single legacy bearer-token verifier.
Authenticated requests retain an opaque verified caller context, including
issuer, audience and expiry, alongside the existing public Principal/ABAC
projections. Those projections remain policy input, not authentication proof.
Explicit skip-auth and trusted in-process embedding have no verified context.

Trusted host composition can install exact issuer/audience-to-domain mappings.
The mapping requires independently configured issuer and audience checks; no
domain is inferred from a subject string or URL. Mapped identity does not grant
Meerkat Recall clearances, live delegation or governed execution. The existing local
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
- Full: 100 tools.
- Default: 26 tools.
- Agent: 27 tools.
- Maintenance: 46 tools.

These are registered profile maxima. Caller scopes, runtime mode, backend capabilities and provider approval can reduce the visible set. New source head and operation tools are metadata reads in the default, agent and maintenance profiles. Source retract/erase and provider reindex administration are full-profile mutations. Scoped search is available in each read profile.

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

Native producer admission reads archive targets only for original Work/Run IDs
missing from its eligible hot observation. Single-record history reads retain
one database statement and use the same hot-first fallback. The admission
transaction still checks every original row and producer payload in order and
writes the same conflict nonces. A physically present changed hot row cannot
be hidden by an archive at commit; archive space, deletion and exact record
checks remain required when the hot row is absent. These observations grant no
cached authority to later admissions or physical provider dispatch.
The lazy archive lookup is a value-returning conditional expression; a bare
SELECT in a statement block would discard the archive and reject valid history.
Duplicate original observations retain their individual nonce writes and shared
erasure revision events, all of which roll back on a later guard failure.

Plain native record SET updates use the same single-record collector as plain
record reads. Target evaluation and permission preparation still precede the
complete UPDATE document pipeline, including index maintenance, events and
default postimage computation. Conditions, explicit output clauses, timeouts,
multiple prepared records and inherited query planners keep general iteration.
This avoids general iterator dispatch on repeated admission nonce and erasure
revision updates; it does not combine or omit any original statement or effect.

See [Work queue capacity and retention](work-queue-capacity.md) for
query-plan constraints, operator settings, and acceptance criteria.

## Preview transport and runtime boundary

`/v1/health` reports process liveness only. Authenticated `/v1/readiness` reports
storage, managed-worker, semantic-ingest and restore-barrier readiness without a
model call. `read:status`, `read:system` or `read:space_config` permits this bounded
status. It does not prove that any requested source revision is searchable.

Managed runtime construction prepares storage and verifies the schema. It does
not start workers or call the local MCP extractor before the API binds. The
listener owner explicitly starts workers after binding. Schema application belongs
to explicit initialization/migration. Shutdown marks readiness false and refuses
new writes before waiting for workers; a timed-out worker drain cannot authorize
a shutdown snapshot. Effective runtime configuration owns the load/save path.

HTTP and MCP support a signed `spaces` ceiling with an explicit route or
`X-Space-Id` selection. An absent ceiling preserves legacy `space_id` semantics;
an empty ceiling grants no space. Selection never chooses the first entry or an
implicit default. Provider approval and current service grants are separate from
the token ceiling. Local development can supply its selected space explicitly.

Public errors carry `error_code`, `message` and `retryable`. MCP responses are
bounded to 262,144 encoded JSON bytes after vector removal; oversized evidence
returns an explicit error and is not silently truncated. The preview contract
examples are in [the source contract](internal/recall-preview-contract/README.md).

## Storage snapshot and restore barriers

Named snapshots use versioned, hashed, finite-retention bundles for both backends.
Export takes the shared storage maintenance barrier after complete producer/work
operations drain and refuses Running work in every space. Embedded replacement
swaps validated complete state; SurrealDB replacement is resumable under a durable
external marker. Readiness and normal storage access stay blocked after an
interrupted restore until erasure replay and final persistence complete.

Live erasure and startup or restore replay first discover every reserved Erasing
head and retain its original journal intent. Existing admitted Space owners resume
first. All original intents finish logical payload and managed-file removal
before one physical checkpoint begins; only then do their original completion
receipts commit under the same exclusive barrier. Final persistence seals those
receipts before readiness returns. Replay keeps the old physical source identity
and never follows an external key to a newly created incarnation.

Native erasure treats the exact bare deferred-index marker as internal state
that requires removal and reconstruction. Full-text reconstruction also writes
a table-local state key outside the ordinary index prefix. Generation checks
permit only that exact key for a selected current Search definition; unrelated
metadata, counters and retained ordinary index history stay protected. See
[derived-index erasure](native-derived-index-erasure.md) for the native key
contracts and validation limits.

Erasure intents are payload-free and retained outside replaced data. Snapshot
manifests bind the independent journal identity. Retained owned backups are
physically removed at expiry cleanup; restored service refuses expired backups.
Managed embedded storage with a persistent destination uses a synchronized
commit WAL and atomic checksummed live checkpoint replacement. Physical erasure
replaces the live checkpoint and WAL before completion, retaining no previous
payload-bearing live checkpoint. Registered raw snapshot exports and their
previous generations are separate backup artifacts with a seven-day retention
bound. Initialization records checked schema-group receipts; startup only verifies
them. See [Snapshot durability](snapshot-durability.md) and
[managed embedded commit durability](embedded-durable-commits.md) for mechanisms,
limits, retention timing and independent journal custody.

The retained historical plain backend transfer verifies destination-owned
canonical epochs and empty original-journal reference guards after its typed
copy. Modern snapshots containing preview control authority are refused before
target mutation; this path does not convert that authority into a new journal.
Online snapshot export uses the configured RuntimeManager to pause new data requests, signal worker shutdown, and wait at most the requested 1-60 seconds for worker drain and the storage activity barrier. Already admitted HTTP/MCP operations retain a shared activity guard through publication; snapshot and source-erasure operations own their exclusive barriers. A drain timeout leaves data admission closed and cannot count as a successful snapshot. The export task survives caller disconnects and returns a checked manifest with bounded retention (default seven days, maximum thirty days). `/v1/system/snapshot/restore` (and retained `/import`) remains available to explicitly global operators when a durable recovery marker blocks ordinary data access. Successful restore replays erasure intent and rotates epochs before workers or data admission resume.

Storage startup takes exclusive writer ownership for the effective embedded snapshot destination even when no snapshot exists yet. Normal serve only verifies schema receipts; explicit runtime initialization and the migrate command apply schema. Readiness does not become healthy while restoration, erasure replay, worker drain, or snapshot quiescence remains active.

Schema verification also recognizes the exact optional fake-embedding repair
receipt written by `migrate` when a real embedding provider is selected. That
maintenance receipt does not establish schema initialization or replace a DDL
receipt. Unknown receipt names, changed checksums and incomplete or noncontiguous
schema history remain errors; verification never rewrites the stored receipts.

Migration 037 appends source publication manifest history for unpublished configuration replans. Existing manifest migration 035 keeps its checksum; the current manifest remains the sole active plan. Serve refuses a missing 037 receipt without creating its table, and explicit initialization applies the appended migration while preserving prior receipts.

Migration 038 connects source visibility/support/mapping/completion/manifest changes to the existing processing index generation. It follows 037 and invalidates staged reindex coverage atomically without introducing a second generation authority. A transaction fence nonce alone does not change indexed content. Like 037, it is applied only by explicit initialization and required by schema verification.

Native staged-node vector writes recheck the complete observed processing
configuration and write a fresh nonce to the existing same-space canonical epoch
in the vector transaction. Dynamic embedding partitions have no canonical
predicate event, and an unchanged configuration assignment cannot establish a
native write conflict. Missing epoch state refuses the vector write. This fence
does not change the processing generation, active profile or reindex progress;
configuration changes already write the same epoch through migration 031.

Grounding and publication details are specified in [grounding and promotion](grounding-and-promotion.md) and [whole-source finalization](source-publication-finalization.md). Evidence preserves exact source/document/revision/node identity, UTF-8 spans and quotes, and inherited security/subjects. Independent support verification uses its approved binding. Node plans stage until the complete source manifest proves all expected effects and search coverage. Entity reuse requires the complete security, subjects and source-support set; identity resolution remains an explicit workflow.

[Maintenance atomic commits](maintenance-atomic-commit.md) retain truth maintenance and resolution workflows. Claims, slots, conflict groups, proposed semantics, audit, outbox and receipt commit under one canonical transaction with exact read-set, epoch, source support and current attempt checks. Deferred adjudication cannot write ahead of its claim commit. Adjudication memo identity includes semantic inputs, evidence, predicate semantics and adjudicator version; unchanged claim IDs alone do not suppress reconsideration.

Source finalizers treat the exact typed `canonical_epoch_changed` and
`canonical_inputs_changed` conflicts as local preparation contention when no
promotion disambiguator was consulted during that execution. These cover both
early preparation and a rejected epoch at the final atomic commit, including
changes made by other source publications or maintenance. The worker waits five
seconds and reclaims the accepted work without consuming its provider retry
budget. Each execution rebuilds the plan
and rechecks the publication, lease, source, configuration and canonical fences;
it never reuses the rejected plan or repeats extraction. Once disambiguation
has begun, even if it fails to return a verdict, the original finite retry
budget applies. Other conflicts and workers retain their existing retry policy.
The commit-time controls create an intervening real canonical transaction on
each backend, require the old plan to leave no effect or receipt, and publish
only after fresh preparation. The epoch remains a required commit fence.
The queue acquires each batch's leases serially before dispatching the claimed
work concurrently. A failed claim precedes execution; it does not produce an
execution failure or an execution-budget refund. The finalizer contention
regressions use this same boundary and check retry accounting against the
successfully claimed attempt, while requiring all fourteen accepted sources
to reach their original publication receipts on both storage backends.

[Canonical outbox allocation](canonical-outbox-sequence.md) preserves the space's
original event sequence floor before replacement effects retire events. Canonical
publication, knowledge-view batches and standalone outbox writes reserve above
current events, the durable retired-event floor and any explicit restored events.
Reservation shares the event transaction; exhaustion rolls back the complete
batch and exact receipt replay does not allocate again. The
[durable event floor](event-sequence-floor.md) in `outbox_checkpoint` is private
producer bookkeeping, separate from successful consumer acknowledgements.

MCP snapshot tools use the same named snapshot format and retention limits as HTTP. The HTTP host supplies a snapshot coordinator so MCP requests pause admissions and drain managed workers before the storage barrier. Native MCP embeddings use the storage barrier directly and fail explicitly if workers remain active. Both paths preserve global operator authorization and durable erasure replay; importing is never an unguarded live dataset replacement.

Source admission requires configured, writable, recovery-ready storage and current caller authority. It acknowledges durable queued work and therefore remains available in the API-only/server-only topology without managed workers. `semantic_ingest_ready` remains false until actual execution capabilities are ready, and source operation/coverage state alone proves eventual completion. Both retained `ingest_doc`/`extract_doc` and the versioned source PUT follow that admission rule. An API-only runtime does not claim that it owns or has started managed workers.

Managed embedded services use a private checkpoint and fsynced transaction journal before acknowledging mutations; see [embedded durable commits](embedded-durable-commits.md). Explicit memory-only library/test clients remain possible. This recovery mechanism is distinct from named export backups and their retention manifests. See the [consumer operational contract](internal/recall-preview-contract/homecore-operations.md) for named-space grants, processing approvals, source reconciliation and readiness.

Provider dispatch contexts can add input guards without replacing the attempt
owner's durable accounting observer. Admission finishes before added guards run,
so input checks observe changes during a durable wait. Guards run in insertion
order on each physical request; usage and completion keep their original owner
and call identity. A guard refusal creates no synthetic usage or completion.

Recall-governed agent builds explicitly disable provider-hosted web search.
An approval for extraction, support verification, identity or promotion
resolution, truth maintenance, wiki generation or verification, or media
rendering does not authorize an additional hosted search. The supplied MCP and
function-tool dispatcher remains intact under its existing authority. This
policy belongs to Recall's governed build seam; it does not change defaults for
other Meerkat consumers or add a public configuration field. Direct single-turn
model clients do not compose the SDK agent's hosted-search defaults.

Multimodal rendering retains the exact managed asset row returned by the payload
read, decoded-byte hash and length, attachment identity and complete policy
partition. Source-backed input uses the existing immutable render-input selector
and the original Work's execution ancestry when present. Direct source-operation
calls retain their source proof. Every actual SDK request, including retries,
rechecks these original observations after durable accounting admission. The
same proof is checked after the provider response before returning a projection.
A replacement row, deleted asset, changed payload, source lifecycle change or
revoked original authority refuses the operation without adopting newer input.

The existing source-less direct renderer primitive remains explicit: no original
Work, no source link or publication marker, and an exactly matching legacy Doc
or global physical Doc absence. It retains the supplied document and compatible
asset policy partition and rechecks that same presence/absence. It creates no
Standing or Independent execution authority. Managed calls use the additive
`Renderer::render_for_work` entry point; missing custody cannot select the
unmanaged path.

Scoped search preserves evidence `span` and `quote` as exact UTF-8 bytes in the
accepted supplied source. The default text renderer and retrieval adapter share
one deterministic transform and original-byte mapping. Each evidence item also
carries the original immutable render's `rendered_span`, selected-slice
`rendered_quote_sha256` and full `rendered_text_sha256`; both digests use plain
lowercase SHA-256 hex. The existing internal revision ID identifies the full
immutable revision and its renderer metadata. Search verifies both raw source
hash declarations, the entire rendered text and supported renderer identity,
current completed publication, and original evidence again before returning.
The raw cover retains any removed interior whitespace. Synthetic-only support,
unknown transforms and invalid UTF-8 boundaries cannot produce evidence. All
additional fields count toward the unchanged encoded response limit.

Contact withdrawal follows the recorded input-support path to the original
independent receipt when restoring an exact prior row. A verified projected
contact root remains the fallback when older receipt history was erased; it
does not replace an available original support ledger during inverse recovery.
Retraction leaves immutable publication completions as history. A newly accepted
unpublished revision may replace that source's historical progress lookup only
after verifying its exact completion and preserving the lookup transaction
comparison. Historical progress never makes an unpublished head visible.

### Versioned English BGE retrieval inputs

The opt-in `bge-en-retrieval-v1` profile uses the standard fixed English BGE
instruction for query embeddings and leaves document/reindex text unchanged.
Only the approved English BGE v1.5 FastEmbed model/dimension combinations accept
it. The instruction is included in tokenizer capacity checks and actual token
usage. Complete transformed inputs must fit; this recipe never truncates them.
The role comes from each request's approved dispatch and is rechecked before
physical inference, including after model and blocking waits. Cached model
resources contain neither role nor authority.

The manifest, profile and vector partition all record the explicit recipe.
Existing `text-v1` and `text-full-v2` behavior is unchanged. A new immutable asset
manifest/profile and checked reindex activation are required; stored vectors are
never relabeled. Readiness identifies the recipe through the existing partition
and operation provenance. The offline provisioning command accepts an explicit
`--preprocessing` selector and preserves its `text-v1` default. See the
[preprocessing and operator contract](embedding-preprocessing-contract.md).

### Verified local model lifetime

Fresh query and worker dispatch handles share a bounded local model resource
owner through their storage client. The owner retains at most one verified
FastEmbed model, keyed by the exact approved local endpoint, manifest digest,
model, dimensions, and preprocessing contract. Concurrent requests for that
identity coalesce initialization. A different identity evicts the retained model
and waits for its executing owners before loading a replacement. The capacity
permit stays with the actual model mutex, including blocking inference after
request cancellation. Cancelled initialization retains its slot until the
constructor completes, so another requester cannot start a duplicate load.

The resource contains no request authority, credentials, observer or input text.
Each fresh dispatch handle installs its own approval resolver and accounting
observer. Approval is still reloaded at dispatch and after model-lock and
blocking-pool waits before every physical inference batch. Sharing verified
immutable bytes does not share space policy or authorize another operation.
Missing or altered assets fail on initial load; a warmed resource uses its
already verified in-memory bytes until replacement or client shutdown.

Deterministic constructor/lifetime fixtures establish coalescing, bounded
replacement and cancellation ownership without a model runtime. They do not
establish ONNX performance, RSS behavior, or installed-model readiness; those
require the separately provisioned model qualification.

The opt-in `fresh_contexts_reuse_actual_model_constructor_and_keep_request_receipts_separate`
regression uses an operator-provisioned `RECALL_TEST_FASTEMBED_ENDPOINT` file URL
and exact `RECALL_TEST_FASTEMBED_MANIFEST_SHA256`. It constructs fresh production
repositories, dispatch contexts and governed handles, checks the actual successful
constructor count, distinct measured receipts, and revocation of a warmed handle.
It never downloads assets. Existing approved local assets can be used during
the affected provider validation. Alternating between two approved local model
identities reloads the single retained slot; this bound does not promise warm
performance for mixed-model workloads.

The deterministic warmed-mutex fixture calls the real dispatch guard with a
synthetic resource; it does not execute FastEmbed's model-batch method. The
installed-model regression revokes before `embed`. Neither test establishes
revocation while an actual ONNX inference is queued. The installed regression
checks usage attribution against each request's admission ID and provenance.

### Work execution after lease renewal

The worker dispatches the exact work item returned by its owned lease renewal.
Only lease expiry, update time, and cleared retry bookkeeping may differ from the
original claim; changed inputs, security or attempt ownership refuse dispatch.
The original source-input fingerprint and publication guards remain in force.
See [render execution after renewal](internal/recall-preview-render-renewal.md)
for the deterministic regression scope and supported operator retry paths.

Recognized source-custody conflicts record only the static
`original_work_authority_changed` context code in failed-run metrics and warnings.
Arbitrary nested errors are not copied into that context field.

Physical source erasure removes every support entry naming a canonical record
that the purge actually deletes, including another source's audit support. Replay
uses the original journal's typed deleted identities to remove orphaned support;
a contradictory surviving canonical row refuses cleanup. Retained independent
facts keep their surviving support. See [source erasure support closure](source-erasure-support-closure.md)
for the physical and interrupted-purge regression scope.


Source embedding preparation and writes preserve the original authority while
reconciling concurrent source-operation progress immediately before their existing
transaction checks. Preparation performs this step after loading the original
ancestry and immutable inputs, before binding or calling the provider. Persistence
performs it again against the same authority after inference. This
reconciliation reads the original serialized operation control and permits only
`receipt.state` and `receipt.error_code` to differ while both states remain live
(accepted, processing or retryable) and completion remains absent. The original
producer, authorization, input/request fingerprints, source incarnation, revision,
acceptance time, published revision, predecessor and contact identity remain exact;
unknown fields are retained in the comparison. Unchanged historical ancestor
controls keep their existing meaning. Source, document, configuration, grant,
Watch and provider-input snapshots are not recaptured.

Both the preparation check and vector/marker transaction still check the
reconciled whole serialized payload and perform their original physical read
fence alongside source, manifest, configuration and attempt checks. A change
after reconciliation still refuses that transaction. An exact standing-authority
guard refusal can retry the storage transaction only when a fresh read proves
that live receipt progress changed since its preceding guard, with all other
serialized operation fields unchanged. This also covers progress during native
vector partition preparation. At most four guarded commit invocations run, each
retaining the backend's existing transaction-conflict retry bound; unchanged
payloads, changed authority, terminal operations and other error shapes cannot
authorize this retry. Each invocation retains the original source, manifest,
configuration, grant, Watch, input and queue-claim checks. The transaction must
still refuse a real revocation even if benign progress happened concurrently.
These local retries reuse the same computed vectors and do not dispatch a
provider, renew or refund a queue claim, publish the source, complete its
operation or adopt a new delegation. Continuing contention remains an error
under the existing scheduler policy. Preparation creates no vectors or node
markers.

Pending local dependencies use a five-second fenced recheck interval, independent of provider retry backoff. They retain the existing blocked-work transition and must satisfy the original source, configuration, input and attempt checks on a fresh claim. Configuration-blocked recovery and transient/provider timeout backoff retain their existing bounded policies. This removes the default 120-second dependency wait without extending publication deadlines, altering corpus expectations or retrying a provider call. A five-second due interval is an eligibility bound; worker polling and execution capacity can add scheduling delay.

SurrealDB reprocess recovery validates an existing hot or archived handoff without writing
its private producer or canonical epoch nonce. Exact job, producer, security,
subject, provenance and archive checks remain inside the transaction. Only a
missing-handoff repair writes both recovery fences before enqueue. Routine
recovery therefore avoids writing these rows while reprocess publication runs.
Existing terminal outcomes remain terminal; observation grants no attempt.

A terminal executed reprocess-work failure also closes its Pending or Running
public job as Failed. The runtime first persists the failed run and closes the
queue under the original lease; it then uses the existing control-only canonical
transaction for the job, audit and outbox. All-space handoff recovery repeats
that bookkeeping after a crash, including when the original work and run have
been archived. The complete job/work/run snapshots and original attempt identity
are checked; a stale owner, changed evidence, unfinished or missing run cannot
supply a terminal execution outcome. No new lease is issued. Paused and terminal
jobs retain their current outcomes. Existing progress remains unchanged and the
public error is the fixed `reprocess_work_failed` code, not provider text.
Pre-execution failures without a run are not represented as executed failures by
this reconciliation. Source selection, published-source requirements, graph
support checks and scheduler retry limits retain their existing behavior.

Native standing-authority preparation and vector-write refusals retain the same bounded transient retry policy as the existing embedded Conflict result. Only the exact two native query wrapper shapes are recognized; incidental text, typed security errors and data-integrity errors remain terminal. The guard is not classified as benign-only: every fresh claim rechecks original source, grant, configuration, immutable-input and attempt authority. Retry exhaustion still records the existing visible durable failure. No whole-payload fence, provider retry limit or authority is weakened or renewed.

A retried embedding job may dispatch inference again; each physical request remains charged to the existing attempt and usage limits. These bounded retries do not guarantee completion under continuing contention. Exhaustion remains a visible durable failure.

The public source-operation outcome follows the selected queue disposition. A transient or timeout cause remains `failed_retryable` while its finite retry budget permits another claim, and becomes `failed_final` when that budget is exhausted; the original `transient_failure` or `execution_timeout` code remains available. Unavailable input repair also records finality before the queue becomes failed. These receipt changes use the same original publication, configuration and attempt fences; configuration blockage remains recoverable. Consumers poll retryable receipts within their existing deadlines and require an actual completed receipt and published head for success.

Native source-operation progress reconciles a competing live receipt update with
at most four outer storage transaction preparations. Each invocation retains the
storage client's existing bound of four native conflict attempts; this is not a
four-SQL-execution bound. Reconciliation requires the exact
original head and all operation fields, including unknown fields, to remain
unchanged except live `receipt.state` and `receipt.error_code`; a terminal state
or completion timestamp refuses it. Every transaction rechecks the original
attempt and source binding. This local path performs no provider dispatch,
queue claim, attempt refund or retry-limit change. Embedded progress reads and
writes under its existing atomic mutation. Both backends preserve unknown
operation fields when recording progress.

Pending local dependencies persist the scheduler's five-second retry through a dedicated queue transition with a 1–3600-second bound. Configuration retries retain their separate 30–3600-second bound. Both use the same atomic, original-lease-fenced transition and refund the unused execution attempt without resetting durable claim generations. A dependency wait therefore neither consumes a provider retry nor inherits the configuration-repair floor. Persisted work diagnostics identify these waits as `pending_dependencies`; source-operation receipts remain processing while prerequisites are unfinished.

Source publication verifies embedding completion against the retained durable
claim generation and lease token, including after work archival. Dependency and
configuration waits can refund the retry counter, so earlier runs with that same
counter remain history and cannot satisfy the current claim. The completed queue
item must carry that exact fence, and exactly one live run must match it with the
same retry counter, a successful result and a completion timestamp. Missing or
conflicting current-run evidence prevents publication on both storage backends;
this check does not change retry allowances, deadlines or source authority.

Source extraction validates its complete original candidate batch durably before
its producer can finish or enqueue validation/commit successors. A structurally
valid proposal that lacks independent semantic support stays Rejected on its
original WorkRun. The same WorkItem may make one complete fresh proposal under a
new attempt fence, subject to the smaller of two charged attempts and the existing
retry allowance. Earlier charged failures consume that allowance. A prior repair
generation that dispatched a physical request cannot become a free additional
repair through a refundable prerequisite counter. Once a semantic
repair is selected, a timeout or expired producer attempt cannot authorize a third
model attempt. Ordinary transport retry policy outside that sequence is unchanged.
An empty repair cannot replace a nonempty rejected proposal, and no supported
subset is silently published. Downstream validation still rechecks current source
lineage; whole-source finalization still requires the original complete manifest.

The extractor reports its actual effective hard budget before dispatch. The
original budget, source/plan/configuration identity and original start/deadline are
retained as frozen input in the existing run metadata. Repair counts backoff and
previous execution time against that original deadline. It requires complete,
exact-fence physical usage receipts whose checked totals match retained attempt
usage, or proven NotStarted with no receipts. Unknown, estimated, missing or
ambiguous usage admits no additional call and retains existing reservations.
Unknown extractor budgets preserve ordinary extraction but disable automatic
repair. For a selected repair, the existing physical dispatch observer checks
retained usage before SDK retries and further support/fallback calls; incomplete
earlier receipts cannot become zero-cost capacity. Ordinary extraction keeps its
existing provider path, including Gemini auxiliary file requests. Unaccounted
auxiliary receipts remain Unknown and cannot authorize automatic repair. Direct fallback receives only the original remaining
tokens and time. Token/configuration changes cannot lend new approval to an old plan.

Agent construction, execution and support-verification futures are constructed
and boxed through a separate synchronous function before the caller polls them.
This keeps child-future construction temporaries out of the extractor's poll
frame. The futures still run in the original task with its provider authorization,
usage observer, cancellation and original budgets. Direct, agentic and fallback
extraction use the same boundary; it does not create another worker or model call.

The work dispatcher constructs only its selected branch through the same
synchronous boundary. Source-producer loading, repair authorization, currentness
checks and artifact validation also separate child construction from polling,
as do extraction media preparation, usage measurement and provider authorization.
Fallible provider arguments keep their original error before any future is
allocated. These boundaries reduce caller construction temporaries across the
nested path; they do not reset the call stack. Credentials, activity tracking,
usage observers, cancellation and branch bodies stay in the original task, and
the default-stack qualification remains required.

Repair prompts contain the complete original evidence window and a bounded prior
proposal explicitly marked as untrusted data. Prior support receipts are omitted;
every new assertion is independently verified through the existing governed
provider operations. Extraction and verification distinguish facts about proposed
or conditional content from operative obligations and completed events. A draft's
rejection or nonidentity with another document does not establish which terms that
other document contains, nor does a passive rejection identify its actor. Repairs
preserve the complete source-backed content, attribution and modality without
adding such exclusions or publishing a previously supported subset. These prompt
instructions guide independent judgments; they do not prove model entailment or
override the complete-batch validation and publication gates.

Same-live-attempt artifact replay and downstream validation
following a durably successful producer can avoid new model calls. A producer
crash before completion provides no cross-run artifact handoff: a new claim needs
remaining original limits or terminates. Rejection history remains in the existing
source retention and physical erasure scope. Failed run and usage persistence and
settlement precede the source-operation disposition and matching queue transition.
Preflight failures record the source disposition before queue mutation without
inventing an execution or usage record.

The grounded stage oracle checks persisted dependency retry times against the independent five-second local-prerequisite policy: the due time is at least five seconds after failed-attempt completion and no later than five seconds after the dispatcher returns. It preserves the real failed run, refunded attempt, lease fence and original payload, and waits for the stored due time. Configuration-recovery backoff remains separate; the oracle does not rewrite scheduling or advance a clock.

The controlled repair token-boundary case preserves its original 300-token
allowance. Extraction uses 150 tokens and leaves 150 for independent support;
the pinned SDK treats reaching that limit as exhaustion. The control therefore
requires the actual budget terminal, both complete physical usage receipts and
terminal source/queue failure without a candidate, bundle or additional attempt.
It separately preserves the rejected-first case whose original deadline expires
before attempt two and the physical-retry refusal after an incomplete receipt.
These controls neither change the production budget gate nor reset retained usage.

Scoped semantic/hybrid search can reuse the exact caller-supplied query vector
within a storage-client lifetime. The bounded process-local cache checks current
provider authorization and a newly captured canonical epoch around every
lookup/fill. Source erasure and restore epoch rotation invalidate in-progress
read authority; an identical caller-input vector can be reused by a later search
that independently checks the new epoch and current corpus. It retains no source text or
results; every search rereads current index rows and source visibility. Details
and bounds are in [Grounding and promotion](grounding-and-promotion.md#caller-query-vector-reuse).
An epoch-only conflict may reprepare the entire scoped search once within the
original request and storage activity lease. The original selected binding,
profile, account, credential generation, policy and observed source authority
remain fixed; provider or lifecycle changes are terminal for that request. A
second epoch conflict returns the existing retryable error. Both attempts retain
normal provider accounting and the caller's original cancellation/deadline; no
candidate, rank or coverage from the failed assembly is reused. See the
[query preparation contract](internal/recall-preview-query-reprepare.md).

Scoped hybrid search combines the globally ordered lexical and semantic
modalities with equal harmonic reciprocal-rank votes, `1 / (rank + 1)` for
zero-based ranks. Each eligible document receives at most one vote per modality.
This keeps a strong paraphrase or exact lexical match competitive against weak
agreement far down both lists; an offset of 60 had flattened those differences
in small spaces. Similarities, eligibility, selected immutable passages, current
provider approval, deterministic ties and existing candidate bounds remain part
of the same search path. Fusion scores are ranking values, not probabilities or
an assertion of provider relevance. The caller's original query is unchanged.

Lexical-only document, claim, and entity results use the same harmonic rank
helper. Their numeric scores therefore change from `1 / (rank + 60)` to
`1 / (rank + 1)` while their relative lexical order remains unchanged.

Wiki editorial failures log fixed phase and typed error-category labels at
context preparation, writer, draft validation, pre-verifier epoch/authority,
verifier and final verification boundaries. The diagnostic never formats the
nested error payload and does not expose source text or provider responses.
Original errors, authorization/epoch checks and bounded retries are preserved.

Standalone wiki compilation permits at most two complete read-only selected-input
recomputations across unrelated writes, requiring the original definition,
selection, evidence, security lineage, freshness and catalog to remain equal.
Original managed authority and the final atomic epoch publication fence remain
mandatory. Batch planning keeps its single original epoch. Writer/verifier calls
and their budgets are unchanged; changed inputs or continued contention refuse
publication. See the standalone editorial input revalidation contract in
[Grounding and promotion](grounding-and-promotion.md).

When explicitly enabled, the private qualification evidence exporter has a
separate failure lifetime from HTTP/MCP serving. An incomplete capture retains
its original failure response and permanently stops that exporter and its
capability. It does not cancel the server or restart the capture worker. The
server retains its own shutdown and error result; server completion cancels any
remaining private exporter. Capture failure still prevents qualification, with
the original evidence, selection and allocation limits unchanged.


Complete current private evidence snapshots may select exact unchanged row bytes
from an earlier complete same-space maintenance capture. Full current storage
reads, epoch/head/source/restore/erasure fences and complete inventory hashes
remain mandatory. Original immutable pages carry bounded row-index/current-
position selections; new or changed rows use fresh delta pages. The client
rechecks prior page custody and explicit full current membership before accepting
the capture. The 4096 newly written physical-row ceiling and independent 4096-row
logical snapshot ceiling are fixed; no prior capture budget is reset. See the
[private evidence contract](internal/private-primary-evidence.md).

The private qualification exporter losslessly fragments rows larger than one
physical page under its existing page, byte and row ceilings. Consumers verify
complete original row bytes before counting or authorizing them; see
[private row fragments](internal/recall-preview-private-row-fragments.md).
