# Grounded extraction and promotion

Recall keeps an extracted interpretation separate from supported, published
knowledge. Extractor confidence is not evidence, and an exact quotation alone
does not establish that an interpretation follows from it.

The active extractor retains unsupported interpretations in staged artifacts.
An independent, tool-free verification call evaluates each complete claim
against its own exact quotation. The call uses the space's approved processing
binding and records the model and verification contract version. An unavailable
verifier fails extraction; a negative verdict cannot become an approved event,
entity, relationship, or attribute. Validation rejects a bundle containing an
unsupported interpretation so it can be repaired or extracted again.

For a typed boolean literal, the verifier input also carries `predicate_truth`,
derived from that literal alone. A false value denies the complete predicate;
it does not mean the assertion is unsupported. The verifier must evaluate that
denial against the exact quotation, preserving any negation already present in
the predicate and the claim's scope, attribution, modality and time. Missing
information, a proposal or silence cannot establish a false value. Nonboolean
literals have no derived truth field, including numeric zero, the string
`"false"` and JSON null. This descriptor neither rewrites the extracted claim
nor decides support. A negative verifier verdict still prevents publication of
the entire unsupported proposal under the existing bounded repair contract.
The governed verifier identity includes the support prompt's hash, so changed
instructions are bound into newly issued receipts. The persisted claim input
and receipt format are unchanged; this prompt clarification does not rewrite
historical decisions or establish live-model entailment accuracy.

Both active extraction prompt paths require one contiguous evidence quote that
contains the context needed for the complete claim. A later sentence may need
its earlier subject, antecedent or qualification included in the same span.
Repair guidance asks the extractor to revisit that original text rather than
invent a document subject from metadata. Exact byte validation and independent
support still apply to the wider quote; neither widening nor metadata implies
approval. Nullable evidence remains representable for failed interpretations,
but cannot become published knowledge. Direct and agentic prompt builders share
these instructions, including during the existing bounded repair attempt.

A successful support decision is bound to the subject, predicate, object and
literal type, entity types and resolution hints, claimed event time and title,
significance, confidence, immutable revision, node, byte coordinates and quotation
hash. Changing those inputs invalidates the decision. Trusted extraction code
checks the existing receipt before translating node-local byte offsets to
revision-global offsets and rebinding the coordinates. Arbitrary moves, including
to an identical quotation elsewhere in the revision, invalidate the receipt. This verification is a model judgment about source support, not a
proof that the source is accurate or that the claim is true in the world.

Before promotion, Recall loads the source, document, immutable rendered revision
and structural node in the selected space. All must exist and be active. Their
complete security policy, classification, handling restrictions, subjects and
subject-completeness state must agree. The document projection must match the
revision text, the revision's SHA-256 must match its bytes, and its immutable
source-content binding must still match the source. Missing lineage never
receives a default public classification. Older revisions without that binding
need deliberate re-rendering; there is no automatic migration or inferred
source identity.

Evidence uses nonempty UTF-8 byte ranges within the immutable revision. The
range must lie on character boundaries and its SHA-256 must match the exact
bytes. Blank evidence, invented hashes, out-of-range spans and references to
another node or revision are rejected. Offset repair is permitted only when an
exact quotation has one unambiguous occurrence; an explicitly correct range may
identify one occurrence of repeated text. Source ordering remains governed by
the accepted source-head generation, not by a content hash.

All derived records, including entity display names, aliases and identity keys,
inherit source restrictions and provenance. Incomplete source subject metadata
stays incomplete. Enrichment and evidence merging use the same complete
security and subject partition and the same source-support and immutable revision set. Identical names
in independently supported partitions can therefore produce distinct entity
records. A replacement revision also creates a separate supported partition so
obsolete aliases cannot survive by borrowing its new evidence. Explicit identity resolution may relate them; promotion does not
silently pool source-exclusive aliases or choose the first of two policy IDs.
This also lets source erasure remove exclusive derived information while
preserving independent support.

Promotion prepares a typed plan of entities, assertions, relationships or
attributes, events, freshness records and outbox records. Preparation performs
no live mutations. The complete source finalizer combines all manifest-declared
extraction groups against one shared plan. The canonical storage boundary
validates the captured read epoch, source generation, attempt lease and sealed
manifest before publishing effects and receipts atomically. A node worker's
staging success is not complete-source success. A preexisting assertion never
stands for missing downstream effects. Direct promotion without the original
publication authority and complete manifest is refused.

Truth maintenance separates claim membership from decision-input identity.
Memoization includes selected values, confidence, full evidence assertions,
predicate semantics and adjudicator/model/prompt version. It is insensitive to
input ordering and unrelated corpus records. A claim changing under the same
ID invalidates a previous decision. Security and subject partitions remain separate during conflict grouping and
corroboration. An authorized comparison across source ownership boundaries keeps
the union of source prerequisites on the derived decision and records its inverse
effects atomically. These comparisons do not cross access boundaries.

Deterministic tests cover evidence identity, support rejection, changed lineage,
partition isolation, uncommitted effect preparation and fingerprint invalidation.
They do not establish live-model entailment accuracy or exact packaged-artifact
qualification. Provider-backed semantic acceptance and transaction/failure
qualification are separate checks.

The retained extraction, manual re-extraction, pipeline-status, timeline and
truth-adjudication scenario fixtures use source acceptance and the production
work dispatcher. Manual re-extraction supplies a new immutable revision and the
previous head's exact generation; the returned operation, not inline rendering,
determines completion. Their deterministic tier uses an approved loopback
embedding transport with fixed responses. Those vectors exercise dispatch,
indexing and publication plumbing only and do not measure retrieval quality.
The live adjudication tier remains opt-in and requires explicit approved
processing configuration and its referenced credentials.

Cross-source comparison is retained through derived decisions with conjunctive
support and explicit identity reconciliation. It must preserve each raw source's
ownership and undo a decision when a prerequisite source is withdrawn. Merely
matching display names cannot merge independently sourced private aliases. The
cross-source timeline and adjudication scenarios retain their semantic acceptance
expectations; completing the identity/decision authority integration is necessary
before those scenarios can be reported as passing.

Extraction coverage is complete only when the selected byte-range union covers every non-whitespace character of the immutable rendered revision. The planner fills gaps using lower-priority node roles when larger nodes exceed per-call limits. Exhausting node, token, or estimated cost limits fails planning before an artifact is persisted; `coverage_threshold` cannot authorize publishing a partial source. Coverage records the persisted revision hash, UTF-8 length, union byte count (including whitespace inside selections), and an explicit `empty_or_whitespace` reason for a truly empty selection. The source finalizer independently checks these facts against the captured revision. Older artifacts without this proof cannot complete a source. Regression source covers role fallback, every budget cap, overlap/order independence, coalesced gaps, empty text, UTF-8 boundaries, and altered hashes. These tests remain unexecuted until scheduled integrated validation.

Accepted render retries resolve the original `PublicationFence` and immutable
`SourceRevision`, and derive one revision ID from the accepted document identity.
The renderer receives that captured template rather than a mutable document
projection. The first persisted successful render is reused before another
provider call; a changed renderer/input recipe is rejected. Failed renders do not
occupy the successful revision identity. Retained unaccepted rendering still
records explicit supersession, and persistence errors propagate.

Tree nodes have deterministic identities from the immutable revision and node
key. A recipe binding includes the complete tree configuration and revision hash.
Every retry visits the full expected tree, checks existing node shape and lineage,
preserves stored summaries and embedding state, and compares the complete live
node set. A missing structural node can be rebuilt with its original identity.
Unexpected nodes or changed configuration fail replay. Once a publication
manifest exists, a missing recipe root or generated summary requires restoration
of its exact captured record; a new model response cannot stand in for it.
Source and attempt checks at atomic publication remain authoritative. The
joined storage erasure barrier drains active work before inventory and cleanup.
Permanent source-reference tombstones reject late staged creates and updates in
both backends. All grounding staging records retain original source, revision,
document or work-run references so those guards also cover delayed replay.
Render-time checks alone are not the erasure guarantee; concurrent erasure and
late-write behavior still requires integrated qualification.

Replay regression source covers accepted immutable input, no repeat render call,
projection repair, stale acceptance, changed renderer/tree recipes, interrupted
structural writes, unexpected nodes and summary call reuse. These tests have not
yet been executed.

When replacing a source revision, promotion starts with the storage owner's
causal retirement plan before reading any entity or claim candidate. That plan
undoes conjunctive decisions and restores independently supported baselines.
The shared promotion overlay hides physical and soft deletions, exposes restored
values, and coalesces subsequent effects against the original persisted before
image. Restored virtual values never become fictitious live-read dependencies.
The canonical transaction independently checks the required retirement effects;
promotion does not run a second retirement or erasure transaction.

The default plan budget covers the accepted two-million-byte source limit with
bounded work: at most 1,000 extraction windows, each estimated at no more than
3,000 tokens, and a one-million-token aggregate estimate. Raw structural members
inside a coalesced window do not count as extra work items. The original complete
manifest is still declared before fanout, and runtime provider budgets remain
authoritative. Large-input planning regressions cover 1.13 MB and 2 MB inputs
without asserting live-provider semantic quality or spending.

### Extraction guidance precedence

Purpose and explicit extraction skill resolve independently from document metadata, then source metadata, then space defaults. Blank values fall through. Only after explicit skills are exhausted does domain inference or the retained source-type/environment fallback apply. The status resolution trace records candidates actually examined and stops at the winner. These content instructions do not change source authorization, policy lineage, processor approval, or provider capability bindings; those remain governed by the space and captured source envelope.

### Retained wiki publication

All entity, source, graph, and editorial compilers capture the canonical epoch before reading their first input. Standalone editorial compilation can recompute its complete selected-input contract at most twice under a fresh epoch, accepting that epoch only when the original contract is unchanged. Publication passes the resulting checked epoch to `KnowledgeViewRepository::publish_at_epoch`; batch planning retains its single original epoch. The storage owner performs the atomic dependency/source proof and publication fence; the compilers do not create a second transaction authority. Cache reuse recomputes the bounded selection first, then checks exact current dependencies and evidence, so a newly matching record cannot hide behind a previously clean view flag.

`GraphViewCompiler::prepare_context` prepares editorial input without writing an intermediate graph projection. Articles include real underlying dependencies; links to the context view/revision are included only if that context was already persisted. Draft IDs are excluded from the editorial memo fingerprint. The model and prompt descriptors remain explicit metadata dependencies and confer no evidence authority.

Editorial source citations pass the shared current-read validator: real live source/document/revision lineage, exact nonempty UTF-8 span, matching quote digest, matching cited source and assertion, and current publication support. A stale hash, zero span or foreign source is rejected rather than repaired into evidence. Source text is never expanded to cover an unrelated node or synthesized as an assertion's support. The source, document, revision and node security/subjects contribute to the article envelope. Policy IDs are opaque and must match; `default` does not authorize a join across policies. Restrictive labels/handling and all subjects are retained, the `subjectless_ok` exception requires every contributor, and unknown subject completeness cannot become partial or complete.

The writer/verifier and final publication retain the same original epoch. Retraction, replacement, deletion or any competing canonical change during preparation/model work causes a retryable conflict rather than publication from a refreshed fence. Exact evidence, read-only context, semantic memoization, envelope containment, and new-selection cache regression tests are authored in the compiler modules. Independent qualification owns the actual accepted-source mutation-during-writer scenarios and storage publication tests. Runtime qualification has not yet been executed.

Source-view compilation consumes the shared current published source projection and filters staged/unpublished documents and revisions through the same read authority. A newer desired source payload cannot replace published source metadata merely because it is present in the mutable intake row. Source lineage quotes continue to use exact immutable revision hashes.

Retained cross-source TM scenarios now use the real public `merge_entities` operation with explicit survivor/input IDs from known immutable fixture assertions. The handler authorizes all observed/proposed row envelopes and commits the canonical identity rewrite. Matching names never trigger production pooling. S5.19 retains corroboration/adjudication expectations after the explicit Alice merge; S5.14 reconciles its known person/project identities while preserving both independently supported event candidates. The ignored live scenario uses the same explicit operation and remains unexecuted.

### Configuration-versioned extraction staging

A versioned source producer reads `_publication`, `_source_configuration`, and `_source_plan` from its own persisted work-item payload before reading extraction inputs. The plan identity must match the storage-owned `source_plan_id` calculation. Current source head and configuration are checked before model dispatch, after the call, and before artifact persistence; a changed configuration requires its own newly queued plan, never adoption of a newer token by old work.

Planning and candidate artifact IDs bind the original plan, stage kind, immutable revision/node and producing work run. Their JSON data carries the original three context fields; candidate data also records the exact selected byte window. An exact same-run persisted artifact can be replayed without another provider call only when its plan, immutable input, authorization and window still match. Different configuration plans or worker runs have different artifact IDs. The finalizer remains responsible for comparing producer payloads and artifact context to the sole active manifest inside its authoritative completion checks. Unversioned compatibility staging is not upgraded; after manifest replanning, old producer output cannot be relabeled as belonging to the new plan.

The immutable render/tree contract is unchanged: a valid persisted render/tree may be reused only under its original recipe. Changing that recipe requires a new accepted immutable source revision. Pure tests cover stage-ID separation and rejection of partial, foreign-space, or relabeled context; source/configuration recovery qualification remains the independent integrated suite's responsibility.


## Retained re-resolution

`resolution::reprocess::prepare_reprocess` prepares existing-assertion link
changes in the maintenance owner’s original-epoch overlay. The job owner keeps
selection, original attempt, durable intent, outbox and the single canonical
commit. No assertion is inserted or re-extracted and no verification receipt is
manufactured. Current immutable evidence and source envelopes are checked before
resolution. Same-name candidates are restricted to the exact source revision and
security partition; an already supported explicit identity merge remains valid.
Ambiguity uses the governed disambiguator and propagates infrastructure errors;
abstention preserves an already current link or leaves the mention unresolved.

An unchanged resolution preserves approved claims and events. Changed links
partition every affected relationship and attribute’s evidence by resulting
identity, preserving unselected support and typed literal values. Old claims and
all their freshness rows are retired together, and replacements start as
candidates with recomputed freshness. Original opaque policy, source ownership,
subject completeness and subject restrictions are retained. Subject requirements
can only grow. Full event support is retained together when participant links
change; existing titles, times and precision are never copied to a subset that
might not entail them. Because event subjects also serve authorization, prior
subject restrictions remain on replacement events even after identity changes.

Previously unresolved assertions can materialize a relationship or attribute
and an event without minting another assertion. An unmaterialized typed literal
requires the original retained extraction artifact and its independent support
receipt; missing type information fails explicitly instead of guessing a date
as a string. A newly synthesized event uses the existing assertion’s exact raw
claim text and supported time, with unknown precision when the assertion did not
retain precision. `skip` significance remains a promotion exclusion. New effects
flow through the canonical outbox into normal truth maintenance; affected old
subject slots are marked stale in the same plan.

Repository scans page completely and stop with an explicit error beyond 100,000
rows per table; no truncated sweep can report success. Regression source covers
unchanged resolved reruns with unrelated corpus, complete zero-write preparation,
stale revisions, foreign model choices, a race after preparation, shared-support
partitioning and order independence, and typed-date preservation. These tests
are authored, not yet executed; validation remains with the integration owner.


The maintenance dependency `0f85891` validates original claim snapshots against
original assertion links and replacement claims against planned links. A further
ReResolve regression now prepares and commits one complete change on both real
storage backends, then checks the original assertion identity/evidence, sole live
replacement relation/event, exact live freshness targets and current read
eligibility. This source test has not been executed by the grounding worker.


The two retained runtime event tests now use an accepted immutable source,
real queue claims and work-run receipts, the production dispatcher/finalizer,
and the shared loopback embedding transport spy. The refinement source contains
two separately supported quotes in distinct extraction windows of one revision;
the date is present in the second quote. An independent fixed verifier checks the
full supported tuple before attaching a support receipt. The expected outcomes
remain one temporally refined event and two events for distinct exact times.
Tests also check completed operation/publication, exact current evidence, source
and revision lineage, and actual embedding dispatch. No direct candidate-commit
wrapper or synthetic completion is used. Execution is pending central validation.


## Explicit extraction skill assets

A selected explicit document, source or space skill must resolve to an installed,
readable, nonempty `SKILL.md` body before extraction. Configured environment skill
IDs follow the same rule. Missing, ambiguous, unreadable and empty assets return
`PipelineError::ConfigError` with an `extraction_skill_*` reason; the runtime’s
configuration-blocked recovery path can resume after the asset is repaired.
Selection never falls through from a missing document override to a space skill.
No explicit skill continues to use the existing optional inferred default.

Aliases such as `homecore-family-kb` work when they uniquely identify an installed
skill directory. A basename shared by multiple skills is ambiguous; specify the
canonical relative directory path. Exact paths take precedence over basename
aliases. Discovery preserves installed path case and retains every alias match,
so filesystem enumeration order cannot select a different skill. Execution
rechecks the original requested alias before reading the body.

`validate_extraction_guidance_assets(space_config, doc, source)` exposes these
same asset checks for readiness without a model call. No wire DTO or new registry
is introduced. Unit source includes an active extractor-not-called/artifact-not-
created oracle and tests for precedence, ambiguous aliases, empty/unreadable
assets and optional defaults. Runtime execution remains pending integration.

## Accepted effects precede graph resolution

The finalizer can pass its already captured canonical plan to the crate-private
`prepare_assertion_groups_with_plan` entrypoint. The required order is one source
retirement prefix, attachment of the original sealed accepted contact effects,
then graph promotion against that shared overlay. Metadata and zero-group plans
use the same prefix without a graph collector. The finalizer must not append the
contact effects a second time after promotion.

The collector preserves the original epoch, before-images, dependencies, effects
and outbox. Sequential changes to a record coalesce from its original before-image
through the existing canonical plan mechanism; neither contact fields nor later
grounded enrichment are overwritten. Empty/stale epochs, cross-space prefixes
and invalid or foreign attempt fences fail before group processing. Comparing the
original epoch to the current canonical epoch never replaces it with newer
authority; the canonical owner checks it again atomically. The collector does not reconstruct contact authority,
repeat retirement or create a new transaction boundary.

Seeded records still require the same immutable revision evidence and exact
support/security partition for reuse. A contact display-name match alone does
not authorize a cross-partition merge, and the collector never fabricates a
`grounded_promotion` step for a contact record. Reusing a structured contact
capability on a later acceptance requires the intake/storage owner's deliberate
authorized preflight; arbitrary public metadata remains insufficient authority.

Three new planner tests cover preservation and enrichment of a grounded seeded
entity, original absent before-image, original epoch/outbox and zero live writes,
plus refusal of a foreign-space prefix, missing/stale epoch and foreign attempt.
These are source-authored overlay tests;
the intake/finalizer owners retain actual sealed-contact and atomic Graph-mode
publication qualification. No Rust test execution is claimed here.

## Runtime fixture alignment after the first broad gate

Promotion, knowledge-view, wiki and source-view HTTP fixtures register their named
space through the real SpaceConfig repository before capturing canonical epochs.
The production unknown-space and publication guards remain enforced. Wiki planning
fixtures retain their original article, visibility and memo expectations; failed
article compilation must not be turned into an accepted empty result.

The extraction media-context positive now creates a real Source and routes a
deterministic media renderer through `execute_render_doc`, then selects the actual
video span from that immutable revision manifest. It no longer relies on a
source-less Doc or a hand-written revision with an invalid hash. The fixed renderer
qualifies context plumbing only, not media-model interpretation. The interrupted
tree-replay fixture binds its native record ID instead of interpolating an
unquoted UUID into SurrealQL. Guidance traces report the canonical selected skill
ID while preserving the original requested alias for strict asset lookup.

These repairs are source changes responding to central runtime evidence; their
successor tests have not been executed in the grounding worktree. Native repository
record-shape errors and shared journal schema fixes remain with their owners.


### Retained worker and resolution fixtures

The six retained render/plan/extract/legacy-alias worker oracles now enter via
accepted manual-source intake and the real claimed queue dispatcher. They use
actual render/tree output, original publication/configuration fences and a
strict literal known-answer support verifier. The optional source hint is removed
only in the two derivation cases; original acceptance authority is preserved.
The alias case changes only the type of a real supported candidate artifact.
Assertions still require the planned immutable revision/node, one staged claim,
unchanged outbox during extraction, propagated source identity and a validated
commit bundle. The local transport guard requires zero outbound model requests.
These stage tests stop before canonical publication and do not claim to qualify
embeddings or model semantics. Re-resolution fixtures register the actual named
space before observing its epoch; their graph/support expectations are unchanged.

Source-only checkpoint: these Rust cases await centrally scheduled execution.

Source-view watch-check dependencies capture the stored record's `updated_at`,
not the operation's `completed_at`; these timestamps serve different purposes.
Read eligibility compares the captured record version before publication.

Retained source-stage and timeline fixtures execute accepted metadata staging and
registration before render/extract. They select the current runnable successor,
not an earlier completed metadata validator. Validation preserves canonical
assertions, relations, and the outbox until actual publication; bundle registration
alone is not completion. The chain oracle drives real embedding and finalization
before consuming outbox events in truth maintenance. Large fixture phase futures
are boxed at preparation, acceptance, dispatcher, publication, and TM boundaries
without changing the default test stack size. A type-size oracle measures the
event fixture phases without constructing them. Runtime qualification remains
separate from these source and formatting checks.

S4.8 uses actual accepted source work and a claimed single-stage driver to inspect
the staging boundary. It injects missing support into a real draft candidate,
requires the real validator to reject it while retaining its exact data, restores
only the original supported draft, and then runs queued validation, registration,
embedding and finalization. It never fabricates source revisions, leases, canonical
assertions or terminal operation state. Its retry claim is specifically repair of
staged validation input; transaction fault/race qualification remains in the
canonical persistence tests. Successful staged artifacts remain durably committed
for replay rather than being deleted.

Extraction and support verification box the actual agent-factory and agent-run
futures at every direct, tool-enabled, and fallback phase. This bounds the outer
phase storage without changing provider bindings, budgets, support decisions or
error handling. A type-size regression checks both outer phases without polling
model code; the existing mock-agent runtime cases remain the execution oracle.

Entity and graph-context view compilers resolve cited spans through the shared
current-read authority before publication or cache reuse. The immutable revision,
document, source and optional node envelopes contribute their actual subjects and
completeness; a Complete graph endpoint cannot make Unknown source subjects
Complete. These exact parent versions join dependency fingerprints. Every parent
must fit the root's existing security partition; incompatible policies or stronger
source restrictions cause refusal, with no source-envelope mutation or first-policy
selection. Compiler contract versions invalidate older cached subject contracts.
The demo regression retains its original Unknown evidence and checks source-only
subject inheritance and refusal after an actual source classification increase.

Stage fixtures compare the actual renderer's complete output, including its final
newline. Planned windows cover the immutable rendered revision's bytes; source
input length is not a substitute for rendered span length, and evidence is never
trimmed to make a fixture pass.

Promotion boxes its nested source-lineage, entity-resolution, event-refinement,
claim-freshness and group preparation futures. The original preparation order,
shared overlay and canonical finalizer authority remain the same. A type-size
oracle bounds both group and wrapper futures; this is a storage-size check and
requires separate execution of the retained event/publication scenarios to
establish runtime stack behavior.


The shared E2E pipeline driver retains each claimed attempt's exact fence and
lease token in its WorkRun, including failed dependency attempts. Only the
production PendingDependencies class uses the production blocked retry/refund
transition. It executes other due work first, then observes the persisted real
run_after (at least 30 seconds); delayed work is never mistaken for quiescence.
Original failed runs remain failed and no readiness/completion is synthesized.
Other failure classes terminate the fixture. A 300-second elapsed deadline,
caller pass ceiling and complete bounded 10,000-item inventory prevent endless
polling or partial-inventory success. Repository regressions check retained
failed-run authority, refund at attempt 901, refusal of an early claim, and
terminal treatment of other typed failures despite incidental dependency words.
These new tests are source-complete; central integration owns Rust execution.

The optional source-hint stage regressions now create a distinct original source
intent with the hint absent before its first enqueue. They retain the accepted
publication/configuration and completed predecessor/metadata references, then
cancel the unused unclaimed alternative through the guarded queue transition.
They never mutate a stored producer attestation, claim or manifest. These two
oracles stop at planning/extraction; they do not certify final publication of a
replacement work graph.

The driver regressions initialize their own memory storage/schema so the same
shared source also compiles when imported by HTTP/MCP fixture crates. The five-
minute elapsed ceiling includes real dependency backoff: the previous central
full-text Alice run took about 162 seconds before these required waits. This
ceiling is a stuck-fixture bound, not a search or model performance claim.

The render-read regression uses the real accepted-source dispatcher, known-answer
support verifier, controlled embedding transport and finalizer on both backends.
It starts with raw CRLF/trailing whitespace, preserves the exact accepted raw
Source template while reading its normalized Doc/revision projection, and refuses cloned
text (even with a recomputed hash), renderer-version and Doc-title substitutions.
A genuinely accepted but unqueued replacement must retain the original published
Doc/revision while hiding its pending Doc. Only real replacement dispatch and
finalization switch current read eligibility. No test creates completion proofs,
manifests, canonical assertions, or a synthetic published head. These are source
oracles for the exact render-proof family, pending central execution.

After actual pipeline completion, the same render oracle now snapshots the persisted original private source-revision payload and completion payload. It separately changes immutable Source metadata and the actual rendered-revision completion fingerprint, verifies that the mutation persisted, and requires both Doc and revision current reads to refuse. It restores only the exact captured payload bytes (without finalizing or rerunning anything), then requires the original positive reads again. IDs, types, source head and accepted generation remain unchanged. The private helper is confined to this negative test module; native storage updates only payload and embedded storage restores the exact original row envelope. This exercises storage current reads directly, not HTTP/MCP transport. Both backend source tests retain the real accepted-R1/pending-R2 transition; no Rust execution has been performed locally.

Two additional backend oracles extend real render/publication preparation through
an actual `WikiGraphPlanner` definition commit. The fixed planner selects the
original Alice entity from the independently verified employment sentence and
creates navigation through the normal canonical writer. It deliberately has no
article writer; an article failure remains explicit and is never called a
successful article. The test reads the actual definition and its canonical
receipt, checks original definition visibility, then applies the existing
persisted immutable-source/completion corruption controls to that definition too.
Exact original payload restoration must restore its positive read.

Genuine R2 acceptance, before dispatch, must preserve the sealed published R1
definition. Raising the live Source classification must hide it; restoring the
original source envelope must restore the pending-generation positive. Real R2
pipeline finalization then invalidates the original R1 definition. These tests
depend on the coherent published-source definition receipt and completed-render
proof families. The two existing render-only tests retain their original scope.
No local Rust tests have run, and these direct current-read tests make no HTTP,
MCP, provider-quality, or concurrent native transaction qualification claim.


Wiki planning inventory now returns exact typed canonical dependencies to the
maintenance planner. Candidate rows must equal their actual stored originals.
Every contributor passes current support checks under the original security
partition, including disconnected inventory entities used by the organizer.
Relations, assertion counts, attributes, events, truth signals and their actual
predicate governance rows enter the read set only when they contribute; complete
scan fingerprints still cover filtered rows and the caller's original canonical
epoch fences changes and phantoms. Current immutable evidence parents and source
envelopes contribute their exact snapshots, subjects and source lineage. The
shared canonical input-envelope helper supplies implicit subjects consistently
with the storage seal. No different-policy union or stronger input is accepted.
Repeated identities with changed content fail instead of selecting a first row.
The planner owner captures the epoch before inventory/provider work and owns the
single canonical publication transaction; this inventory creates no receipt or
alternative publication authority. Candidate order does not change the captured
read set or fingerprint. New source tests cover stale/unpersisted candidates,
disconnected organizer inputs, conflicting duplicates, and unrelated security
partitions. Existing inventory positives persist their original inputs first.

Managed graph compilation retains the exact prepared invocation authority supplied by the wiki caller. The same opaque proof gates lookup, the first context read, every prepared or cached return, and the final guarded publication transaction. It is never recaptured from a successor claim. The direct-library constructor remains explicit; managed callers attach the original proof with `with_managed_authority`. The graph read oracle exercises both backends using actual guarded claim/requeue/reclaim transitions, refuses cross-space use, and proves that a valid successor cannot revive the original compiler. Missing-definition reads are a sentinel for checking refusal before graph inputs; they are not a completed-publication positive. Storage and editorial tests own final transaction and provider-interleaving coverage. Source authored; no local Cargo execution.

Wiki inventory distinguishes the live raw Source used for original commit CAS from its eligible published immutable projection. Both exact policy/subject/provenance envelopes constrain the planned definition; only the raw row occupies the Source dependency identity. Original source parents from both envelopes are visited with a deduplicated queue. A pending desired replacement therefore cannot substitute its content for published evidence or erase the original published policy lineage. The canonical definition owner seals and validates the published revision in the same transaction; this inventory does not mint that proof. A source-level envelope oracle retains the raw dependency, unions both subject sets and original parent IDs, and refuses a foreign published policy. Full accepted-R1/pending-R2/read coverage belongs to the joined definition receipt tests.


The direct multimodal renderer rejects a document/context space mismatch with
`ConfigError` and the `provider_authorization:` code before inspecting attachments,
loading managed media or constructing a provider request. The scheduler and media
fetch authorization remain independent checks. The actual-adapter regression is
in semantics checkpoint `c2900def1c264e2cb8defeb5f86fc72eb794cae8`: a real
managed PNG and the live governed renderer verify zero sends for mismatch (also
with an absent asset), missing approval and revocation, plus the authorized
single-send path on both transports/backends. Those are source oracles pending
central execution; this guard makes no runtime or stack-size claim.

Source dependencies in entity, graph and editorial projections now describe the
exact published immutable Source snapshot. The compiler first verifies its full
raw Source read still matches storage, then resolves that row through the same
`ReadEligibility` authority. It inherits both raw and published source subject
and provenance envelopes, while persisting only the published timestamp for the
semantic Source dependency. Live normalization metadata, lifecycle adornments or
pending R2 input therefore cannot masquerade as the original published payload.
The strict storage-side Source dependency validator is unchanged. Editorial
parent reads must fit the original classification/label/handling envelope; they
cannot silently widen that envelope. Redundant unversioned Source dependencies
are removed. Entity/graph/article compiler versions advance to 6/5/17.

The actual pipeline/definition tests now additionally update only a live Source's
normalization metadata and timestamp, prepare a real read-only Graph context,
and require its Source dependency to use the original published timestamp. They
restore that exact original live row, retain immutable-control tamper negatives,
and require the same original Source dependency while real R2 is only desired.
No context publication, source head replacement or new receipt is fabricated.
The shared e2e/scenario fixture gains an explicit knowledge-views dependency;
its two lockfile edges are updated as source without invoking Cargo. These tests
remain source-only and depend on the coherent held Wiki/render group.

The retained multi-node event and extraction-to-truth fixtures now consume the
shared worker driver's real `PendingDependencies` result. A deferred stage must
have its original failed WorkRun, source generation/revision, persisted attempt
fence, refunded attempt count, cleared lease and at least thirty-second retry
delay. It remains Processing with no published revision. Every other failure
class remains fatal, and stage-specific assertions still require a successful
run. The bounded publication driver distinguishes delayed runnable work from an
empty queue and waits for the original due time, within a single 180-second
fixture deadline; it never edits `run_after`, refreshes source authority or
manufactures completion. Existing final event, exact evidence, original source
publication and truth-maintenance assertions remain unchanged. These are source
repairs for the three Runtime11 deferred-finalizer failures; Rust execution is
reserved for the coherent integration checkpoint.

Optional section summaries now capture their actual input before the first tree
write: the current raw Source, current Doc, exact immutable DocRevision, original
accepted Source and publication fence. All applicable original standing
principals must authorize every current input and the separate immutable Source
envelope. The explicit `execute_build_doc_tree_for_work` entrypoint follows the
original claimed RenderDoc/BuildDocTree execution and exact payload input IDs;
it retains the original finite lease and distinguishes independently admitted
work from inherited Watch execution. The existing direct library entrypoint uses
source-operation authority, or the retained unaccepted no-SourceHead path.

The default summarizer composes this fixed input guard after the original
provider observer's durable admission, preserving usage/completion accounting.
It revalidates at the physical dispatch callback, immediately before summary
execution and after the response before storing a summary node. A changed input,
current source generation, current authorization or original lease refuses the
response. Created root/section nodes are outputs rather than summary inputs, so
ordinary tree writes and unrelated corpus changes do not invalidate the fixed
input set. No global epoch is refreshed and no transaction authority is created.

This source checkpoint depends on B's original standing source/Work proof API,
I's pure all-principal input authorization and S's observer-composition method.
B owns the managed runtime callsite. New two-backend source controls alter the
actual revision (with a recomputed hash), Doc and Source independently, exercise
the actual observer callback and unrelated-corpus positive, and mutate a Source
during an injected summary response to require no summary node. These controls
have not been executed; installed SDK/standing revocation and native atomic
publication remain separate integration oracles.

The same immutable render-input resolver is crate-visible for the media adapter;
its source/current-generation checks are shared rather than reimplemented. This
visibility change does not authorize the separate direct unmanaged renderer or
replace the caller's original Work, raw-row and managed-asset guards.

The render entrypoint also carries the original claimed Work through
`execute_render_doc_for_work` to the renderer's additive `render_for_work`
method. It captures the current raw Doc separately from a caller's original
pre-render argument, retains the same source/operation/attempt proof, checks it
before a cached render updates the Doc projection, and checks it after rendering
before persisting a revision. The shared fixed-input helper accepts an optional
revision so render and summary paths use the same authority logic. The direct
legacy fallback additionally requires the source-head owner to confirm physical
global absence; a foreign or malformed row cannot masquerade as no SourceHead.

Two further source controls run against both backends: a renderer that changes
its actual source must be invoked once and leave no immutable revision, and an
already cached accepted render must still reject unclaimed work without changing
the original revision. The renderer trait/physical media guard belongs to S,
the actual managed caller and global head-presence adapter to B. No Rust tests
have been run for this held Standing family.

The render/summary original-Work guard also binds every supplied source ID and
publication fence to the selected immutable source input before following the
Work's private authority. A changed generation, operation, revision, source or
malformed fence refuses even when `doc_id` still matches. Missing optional fields
remain subject to the existing admitted-Work authority and claimed-lease proof;
they do not establish Independent origin. Source tests retain a real accepted
source and exercise the descriptor positives and all fence/source mutations on
both storage backends; these descriptor controls do not claim execution authority.

The default text renderer and public evidence consumers share
`elephant_types::rendered_text::normalize_default_text`. It preserves the existing
`elephant.renderer.text` 1.0.0 output bytes while retaining exact original origins
for copied UTF-8 ranges and normalized CR/LF. Generated terminal LF has no source
origin. `MappedText::raw_cover` returns the smallest contiguous original cover;
removed whitespace inside that interval is retained in the public raw quote.
Equal phrases are never searched for, so later occurrences retain their exact
positions. Empty, reversed, out-of-bounds, non-UTF-8-boundary and wholly generated
intervals refuse.

`original_text_span(original, rendered, renderer, span)` accepts only the pinned
default transform or exact passthrough 1.0.0, both with no configuration hash, and
compares the complete rendered text before returning a raw interval. It does not
authorize evidence: the retrieval owner must also verify original source bytes
and hash, original immutable revision/hash, complete publication, exact evidence
and current parent policy. The existing public raw span/quote meaning remains;
C owns the additive rendered coordinates/hash and S the active search consumer.

The source tests consume D's independently authored seven literal coordinate
cases (`cb0e449`, render-coordinate-fixtures.json), cover refusal cases and compare
the shared renderer against its preserved old transform across line-ending,
Unicode and whitespace combinations. Mapping runs coalesce so long sequences of
unchanged line endings do not allocate one entry per byte. Rust execution remains
reserved for integration, and this coordinate family does not alter the frozen
Runtime12/13 candidate.

`DocTreeConfig` supports explicit serialization and deserialization so the runtime
owner can retain the exact configuration constructed for its worker and supply
that same value to render/tree execution and the bounded effective-recipe
snapshot. The tree consumer does not reread environment values or derive a
configuration receipt from its replay digest. Runtime snapshot construction must
select only the approved configuration fields; the whole pipeline configuration
contains credentials and must not be serialized into qualification evidence.

The retained grounded render-stage fixture also checks worker configuration
custody. It captures `create_paragraphs=false` before real source processing,
passes that same worker configuration through the actual claimed dispatcher,
and requires persisted chunks with no paragraph nodes. It retains its existing
immutable revision bytes/hash, plan and source-fence assertions, plus the shared
original deferred-attempt checks. The old inline unclaimed fixture is not used
for this configuration oracle. This source control requires the runtime owner's
retained `WorkQueueWorkerConfig.doc_tree` field and actual dispatch wiring.

The render and summary mutation fixtures use object-valued document metadata,
matching the native schema. The render response control requires exactly one
renderer invocation and the specific original-input guard failure, so a setup
failure cannot satisfy the negative case. Deferred stage controls compare the
complete original payload plus the actual fence persisted in the original
failed `WorkRun`; claiming legitimately adds that fence. They still require
the original publication/configuration inputs, refunded dependency attempt,
released lease, failed run and production retry deadline without modification.

The real completed-source Definition regression also checks the two Source
authorization envelopes independently. A temporary live-only policy permits
the current control row while denying the unchanged immutable published Source;
the reverse clearance denies only the live row. Both reads must refuse, and
allowing both policies returns exactly the original immutable Source. The test
restores the original control row without resealing completion or changing any
accepted revision. Its graph context also crosses the real publication validator
with the original captured epoch and unmodified compiler output after benign
control normalization and during pending replacement. These controls use the
actual accepted source pipeline on both backends; they are not provider-quality
or independent policy-engine qualification.

Direct and agentic extraction keep the independent support-verification future
behind a heap boundary when awaiting it. This limits the enclosing extraction
poll frame while retaining the distinct current SupportVerification approval,
original budget, observer and tool-free verifier. Bounded phase diagnostics
report parsing, normalization and verifier completion without source text or
provider credentials. The existing real extraction/independent-approval
regressions must verify runtime behavior; source future-size controls alone do
not establish that a stack overflow is resolved.

The completed-source render/Definition regression heap-allocates each storage
projection future and the split-envelope control block. Bounded diagnostics
identify the original publication, Definition preparation/publication and
replacement phases and report actual child-future sizes. A source test limits
the enclosing fixture future to 128 KiB without constructing a backend. These
controls preserve the original two-policy, proof-tampering and pending-revision
assertions on both backends and the default test stack. Runtime validation is
required to localize or resolve an observed stack overflow; the future-size
check alone does not establish a passing Definition regression.

The actual Definition planner future also carries a test-local tracing dispatch
across its polls. It admits only Wiki planning and maintenance-commit phase events
and prints bounded static phase names and numeric future sizes; messages, IDs and
source fields are ignored.
This captures the inner planner's diagnostics without changing the process-wide
subscriber, environment, stack size or original publication assertions. Those
observations distinguish future construction from later preparation and commit
phases; an outer future's size alone does not bound its nested call frames.

Graph context preparation also heap-allocates its actual definition, selection,
assertion, citation-grounding, cache and final-validation child futures. Pure
revision assembly consumes the selected records before the final current-evidence
and original-epoch checks are polled. Citation grounding keeps the same ordered
raw-source equality, published-source projection, envelope consistency and final
read-authority checks; only the awaited child allocation changes. The original
selection limits, subject union, exact policy partitions, dependencies, cache
conditions and publication guards remain in force. Two type-size regressions
bound the six actual graph preparation futures and two shared citation futures
to 32 KiB without constructing a backend. These allocation controls require the
original Definition and evidence regressions on both backends and the default
stack; they do not establish that an observed stack overflow is resolved.

The original populated mixed-source erasure projection exercises the entity-rooted
compiler through `rebuild_knowledge_view`. It does not exercise the governed
Graph-definition compiler. The compiled Runtime24 entity preparation still
reserved 152,088 bytes for its poll frame; a small stored future alone did not
establish bounded cumulative stack use.

Entity compilation now completes three private phases in order: bounded material
selection, revision grounding and validation, and atomic publication. Selection
returns owned boxed inputs before grounding is constructed. Grounding returns
before publication is constructed. Pure revision assembly has a separate,
non-inlined synchronous call boundary, so its local temporaries are gone before
the final current-evidence read. Repository and validation child futures are
boxed within their original phases. The original epoch is still captured before
any input reads and passed unchanged to cache validation or the existing atomic
publication boundary. No epoch is refreshed between phases.

Selection order, limits, exact security comparisons, source evidence, subjects,
provenance, freshness, dependency identities and cache predicates remain the
same. Assembly preserves all original fields, and invalid or withdrawn evidence
still prevents publication. Shared current-evidence and epoch-check helper
semantics are unchanged. The new type-only control bounds actual private future
state without constructing a backend or polling storage. The original entity
reuse and mixed-source erasure tests remain the behavioral controls, with their
original assertions, deadlines and stack sizes. Source extraction and formatting
checks establish source consistency; they do not establish a runtime stack cure.


The original rendered-publication fixtures compare the complete projected Source
with the immutable accepted Source plus its original completion-certified
progress. Their expected value is independent of the projector and current
desired state: the actual original completion, manifest, completed operation and
stored triage decision must agree on the original source, publication and Graph
mode, including the accepted-source and decision fingerprints. Only mode,
GraphReady lifecycle and that certified original decision link are overlaid.
Both independent live/published authorization denials, observation of the exact
immutable security envelope, full Source equality and exact live-row restoration
remain required.

The corruption controls capture the bounded complete physical set: one accepted
revision and both original completion copies. Each completion copy must initially
match the actual original certificate, and each is corrupted independently while
all other captured rows remain unchanged. Reading the original immutable
completion after each corruption identifies which physical copy was changed;
no private storage key is reconstructed. Altered immutable revision or completion
proof must still deny Doc, DocRevision, Source and generated Definition reads.
An altered progress lookup denies Source and generated Definition reads, while
Doc and DocRevision must still exactly match their independently intact immutable
certificate. Thus the lookup does not become a new document authority. Every
control restores the entire original physical set before propagating a failure,
and successful restoration must return the complete original records. No worker
rerun, resealing, new publication or replacement identity supplies recovery.

These assertions run in the existing embedded/native render and Definition
fixtures, including their scenario copies. Source and formatting checks do not
establish their runtime outcome; central validation must execute the affected
fixtures with their original deadlines and stacks.


### Entity lookup dispatch when vectors are absent

The active entity search used during extraction checks current, approved-profile
entity vectors before embedding its query. With no live vectors, semantic ranking
cannot contribute, so the existing lexical result is returned without a provider
request. This also applies when previously stored vectors belong to erased or
changed entities. Approval is still resolved first, including for an empty index.

The availability read is discarded before inference. Ranking independently reloads
current vectors after inference, preserving profile, erasure, restoration and
entity-input guards. No corpus/vector cache or alternative approval authority is
introduced. Three transport/storage controls cover empty-to-populated-to-erased
state, stale entity input with lexical results, and revoked approval on an empty
index. These controls do not establish semantic quality or that every provider
quota in qualification is sufficient.

The availability check preserves `ProviderUnapproved` when approval disappears,
is revoked, or changes profile after initial binding resolution. Ordinary storage
failures retain the existing entity-ranking fallback policy. A fourth control
checks those post-resolution error classifications without opening a provider.


### Predicate maintenance embedding transport

A predicate hygiene preparation submits the texts of all eligible security and
support partitions through one embedding adapter call. The adapter retains its
existing request-size limits, splitting and physical-request admission. The
complete response must have exactly one finite vector of the approved dimension
per input before any semantic grouping proceeds.

Every predicate is observed before dispatch. Returned vectors are assigned back
in exact input order, and similarity groups, alias decisions, draft proposals and
inherited source restrictions remain confined to their original partitions.
Singleton partitions require no embedding. No embedding or decision cache is
introduced; the existing canonical owner commits the complete prepared effects.
This reduces separate transport calls for small partitions without changing
proposal thresholds, source authority, or publication requirements.


### Shared remote request admission

Governed generation streams and OpenAI embedding attempts share two in-flight
slots per normalized remote endpoint within one Recall process. Models and account
aliases at the same endpoint share this capacity. A permit lasts through the full
stream or embedding response decoder; completion, errors and cancellation release
it. Embedding backoff waits without holding a slot. Original caller deadlines,
physical-call accounting, provider approvals and per-request credential checks
remain unchanged; approval is rechecked after admission waits.

This is process-local backpressure. It does not coordinate independent Recall
workers or other applications sharing an upstream account, and it grants no
provider or spending authority. Local embedding models use their existing resource
controls. Idle endpoint entries hold only weak references; no source, query text,
vector, credential, or approval is cached in the admission registry.

Five new controls cover normalized endpoint sharing, queued cancellation and
capacity recovery, embedding and generation approval revocation while queued,
and three real localhost embedding attempts. The last control leaves two response
bodies incomplete, verifies the third request cannot enter, then cancels one
request and verifies the third completes. These are local transport controls,
not evidence of cross-process or upstream-account concurrency guarantees.

A separate mixed-family localhost control holds an incomplete governed OpenAI
stream and an incomplete embedding response at the same endpoint. A third physical
embedding request stays queued until the generation task is cancelled, then
completes; both slots are recovered afterwards. This directly exercises the
async-stream permit lifetime as well as the common admission seam.

### Caller query vector reuse

Scoped semantic and hybrid search reuse only the vector of the exact public
`SearchRequest.query` bytes. Each request still reads the current corpus, source
heads, evidence, visibility, permissions and physical index, then ranks results.
No source text, result, citation or approval is cached. Entity lookup, document
embedding and predicate embedding do not use this cache.

The process-local key is a digest of the exact query digest, named space, complete
current caller projection and verified identity, provider partition/dimensions,
configuration/binding/profile provenance and existing storage-client lifetime.
Unordered caller sets are sorted; clearance order and explicitly empty subject
restrictions remain distinct. Configuration changes and a new storage-client
owner cannot use an old entry.
Canonical graph writes, source lifecycle changes and restore epoch rotation do
not change a caller-supplied vector. They invalidate an in-progress search
through its separately captured epoch and force every subsequent search to
reread current source eligibility and index rows. Query input remains independently
supplied by the caller; the cache does not extract or retain source content.

Fresh provider authorization precedes lookup and follows every shared-fill wait.
The filling request also checks authority and epoch before dispatch and before
installing a result. Physical calls retain existing credential, admission and
usage accounting. Failed or cancelled fills never install a successful value.
There is no dispatch receipt for reuse because no physical request occurs.

Entries expire after 300 seconds from successful fill (unfilled slots from
creation). At most 128 cells and 16 MiB of reserved vector bytes plus a 1 KiB
per-entry metadata charge are retained. Filling/waited-on cells cannot be evicted
around these bounds; a saturated cache uses the ordinary uncached provider path.
These limits bound cache ownership, not buffers owned by active search requests.
Only digests, weak lifetime references and float vectors are retained. This is an
in-process optimization, not cross-process deduplication or a promise that a
particular workload fits its existing provider budget.

Controls cover strict entry/byte/expiry limits, store identity, exact query bytes,
caller and named-space isolation, credential/configuration changes, canonical
epoch reads versus actual restore-authority rotation, cancellation/error fills,
and approval revocation while callers
share a held localhost HTTP response. A real accepted-source/render/embed/finalize
fixture checks semantic/hybrid reuse on both backends, physical source erasure
through a lookup that includes soft-deleted records,
reuse across the new epoch with fresh results containing only the surviving source. These
controlled vectors establish behavior, not model relevance.


### Current entity-vector input guards (source27)

The source26 native original compiled all five selected targets and passed its
first ten exact tests, then the stale entity-vector control failed: SurrealDB
sent one query embedding request where the test required zero. The entity's
summary had changed while its supplied `updated_at` remained unchanged. The
Surreal vector loader compared only the stored entity version; the embedded
loader already compared the canonical text hash. This was a real read-path
freshness gap, not a reason to weaken the zero-request oracle.

The Surreal vector read now projects the current display name, aliases and
summary in the same query as each stored vector and input hash. It checks that
preimage with the shared canonical entity text preprocessing and SHA256 helper
before a vector can make inference useful or participate in ranking. Existing
profile, authorization, entity lifecycle, space and timestamp guards remain.
No new revision authority, schema change or timestamp rewrite is introduced.
Ranking still reloads current vectors after the outbound request; this change
does not claim an atomic snapshot across the request and concurrent writes.

The existing exact stale-vector test now covers both backends, proves that the
input hash changed (and the Surreal timestamp did not), preserves lexical
results with zero physical requests, rejects semantic matches from the stale
vector, and verifies that publishing a vector for the current text restores
semantic lookup with exactly one request. Source26's failed original and its
unrun remaining tests remain literal history. This checkpoint contains test
source only; no successor test result is claimed here.


### Original claim in the index fixture (source28)

The source27 native continuation compiled all five selected targets and passed
its first27 tests, then the source-erasure query-cache scenario failed while
preparing its physically indexed inputs with
`source_embedding_requires_original_claim`. The shared index fixture had a
real claimed `embed_source_revision` job and sealed manifest but passed neither
to its `EmbedWorker`. The generic managed-node refusal was correct; the active
source runtime already captures and attaches the original claim authority.

The fixture now calls the same `prepare_embedding_write` with the claimed
work's attempt fence, original publication, sealed manifest configuration and
rendered revision before resolving its provider, and attaches the resulting
`SourceEmbeddingWriteAuthority` to the worker. Source vector writes therefore
use the existing fenced transaction. No head, vector, completion receipt or
source authority is fabricated. Production code is byte-identical to source27;
the source-erasure scenario's physical deletion, canonical epoch, fresh results,
query reuse and revocation assertions are unchanged. The selected scenario
still covers both retained backends. Source27's failed original remains literal
history, and this source checkpoint claims no executed successor result.


The semantic index fixture also retains the original claimed attempt in its
`WorkRun.metrics.custom.attempt_fence` before embedding, as the active worker
runtime does. Completion verifies the run's space, work ID, retry count and
captured fence before persisting success and completing that same queue claim.
Publication requires exactly one successful completed run matching the durable
completed attempt generation; a success flag and retry count alone are
insufficient. This applies to both retained storage backends.

The complete fixture sequence is acceptance and immutable rendering, a sealed
manifest and registered metadata, an original embedding claim and fenced run,
managed-node embedding under that claim, successful run persistence followed by
owned queue completion, then finalization under the finalizer's own original
claim. The production finalizer retains its source/configuration/manifest,
metadata origin and registration, exact vector/index, and canonical transaction
guards. The physical-erasure scenario's assertions remain unchanged; the fixture
uses production receipt identity instead of bypassing publication requirements.

### Standalone editorial input revalidation

A standalone article may outlive unrelated canonical writes while its writer or
verifier runs. At the existing preparation boundaries it can perform at most two
read-only, complete bounded selector/catalog recomputations. It accepts a new
epoch only if the complete original definition, catalog, graph content, support,
source revision IDs and spans, security/subjects/provenance, dependencies,
freshness and validation still match. Exact source records and optional source
heads are captured explicitly, including for direct library invocations without
a shared request reader. A same-content source-head transition cannot be adopted.
Only generated identities and publication
clocks of an unpersisted intermediate graph projection are excluded from this
comparison. Persisted projection identity remains exact. New matching graph
members, changed/withdrawn support and changed definitions refuse publication.

The original managed authority is retained throughout; revalidation cannot renew
a grant or work attempt. The writer and verifier are not replayed. A writer
racing the recomputation or the final existing storage transaction still causes
a conflict. Batch articles retain their common original epoch. Exhausting the two
recomputations fails closed without adding provider calls or extending budgets.
Epoch verification itself uses the read-only current-epoch lookup and never
initializes missing state or repairs a foreign epoch owner.

Source33 controls cover independent writes during both model phases, selected
entity/definition changes, complete query-membership recomputation, immutable
revision corruption/source withdrawal, bounded recomputation, and a real write
between preparation and atomic publication. They also retain physical erasure
and fresh-result controls for pure caller-query vector reuse. These controls are
source-authored until Root records their actual execution. The closed source32
run's article.contents/storage_conflict did not identify its exact inner guard;
these deterministic source paths do not retrospectively prove that guard.
