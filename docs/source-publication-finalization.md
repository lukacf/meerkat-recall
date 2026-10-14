# Whole-source publication

A source operation publishes one immutable accepted revision. Accepting a replacement
keeps the previous published graph available until the replacement can commit in full.
The accepted head generation, source revision, and operation ID travel with every child
work item and staged bundle. Neither a content hash nor an observed latest head can
replace that captured authority.

Before extraction fanout, the worker records a closed `SourcePublicationManifest`.
It contains the exact rendered documents, immutable document/source mappings, persisted
extraction plans, deterministic future bundle IDs, selected nodes and spans, metadata
receipts, and required embedding nodes. It also pins the approved configuration and
full vector identity and one deterministic embedding job per rendered revision,
including revisions with no embeddable nodes. An identical retry may reuse the manifest. A retry cannot shrink
it to the children that happened to finish, or add children after sealing it.

Node `commit_artifacts` work records a durable registration of each validated bundle,
bound to its original captured attempt. It does not
publish node-local canonical effects. The finalizer reads exactly the declared bundles,
checks them against their original validated candidate payload and security envelope,
and passes all extraction groups through one promotion overlay. This lets identities,
claims, and events from different nodes participate in the same prepared transaction.
The persisted planner coverage must match the actual revision hash, byte length and
merged selected byte ranges. Finalization independently checks that every non-whitespace
character is covered and all span boundaries are valid UTF-8. Budget truncation and a
missing coverage proof cannot certify a graph revision. The finalizer uses its own originally captured queue attempt, distinct from each
extractor's work-run provenance. Each artifact's origin run must have succeeded and
completed, and its work item must be done. Every bundle also needs a registered commit
stage whose done work item retains the same captured attempt. Hot queue and archived
records satisfy the same checks; a stored artifact from a failed run cannot certify
completion. Metadata may register before rendering and plan sealing. Other bundle
kinds cannot bypass the closed plan. A planner retry can adopt its own previously
sealed selections under its new current attempt, without changing the expected set.

Publication checks the expected source head, current configuration and approvals,
complete artifact set, immutable evidence mappings, real vectors and searchable
partition indexes inside the canonical transaction. A node's embedding-model marker
alone is insufficient. The exact embedding job must also be done with a successful
completed run; background vectors do not bypass that obligation. The required vector identity includes the space, provider,
account, endpoint, model version, dimensions, and preprocessing. Missing vectors,
rejected candidates, stale attempts, source deletion, or revoked processing authority
prevent completion. This is storage coverage evidence; it does not establish semantic
embedding quality, which requires separate provider qualification.

The same transaction retires the prior source's obsolete support, preserves independent
support, writes all new effects and outbox events, commits the expected artifacts and
candidate and planning records, applies normalized source metadata and the selected
lifecycle projection, records a completion
certificate, and advances the public operation receipt and published head. The
completion certificate is diagnostic evidence; the source head and operation receipt
remain the state authorities.

The selected ingest mode defines the required effects. Explicit skip preserves the
skipped lifecycle and still completes its metadata obligations. Metadata mode requires its
validated metadata receipts. Retrieval mode adds rendered documents and complete index
coverage. Graph mode also requires every selected extraction bundle. An explicit
persisted plan for an empty or whitespace-only revision is different from an absent plan
or a temporarily empty queue. Even that explicit zero-node graph outcome requires its
metadata and retrieval obligations. Nonempty content with an unreasoned empty selection
remains incomplete. Deliberate policy exclusion needs durable authorized coverage
evidence; a budget cap or empty selection alone cannot certify it.

Recovery scans durable manifests in every named space. A crash between manifest creation
and child enqueue leaves enough information to reconstruct missing work. A crash after
the last bundle or last embedding leaves the operation discoverable without a new
request or a last-child notification. Incomplete finalization is retryable; it never
means an empty successful publication. After a successful transaction, replay returns
the complete canonical receipt before attempting semantic preparation again.

A concurrent canonical writer can invalidate the planning epoch after promotion
has prepared its effects. The exact typed `canonical_inputs_changed` refusal
from retirement rechecking or the final transaction receives the same local
dependency retry as the early `canonical_epoch_changed` refusal, provided no
promotion disambiguator was consulted. The rejected plan publishes nothing.
After five seconds the queue can claim the work again, under a new attempt fence,
and rebuild the complete plan from fresh reads. This does not refresh the old
plan's epoch, relax any source or security check, or rerun extraction. A consulted
disambiguator, including one that returned an error, retains the finite charged
retry budget. Other conflict and error classes keep their existing policy.

Deterministic commit-time tests on both backends insert a competing canonical
transaction after the original input reads. They require a typed refusal, no
partial effect or receipt, unchanged post-winner epoch, and a successful fresh
preparation. The active dispatcher tests separately retain all fourteen original
accepted publications and their exact completed receipts. Authored tests are not
evidence of execution or a completion guarantee under continuing contention.

Documents and rendered revisions map explicitly to the accepted source revision.
A replacement uses a new document identity, preserving exact old published evidence
while the replacement is pending. Retrieval checks those mappings against the public
published head; a mapping by itself grants no visibility. Node support resolves through
its immutable rendered revision. Text source offsets remain tied to the accepted input;
media uses its rendered revision and explicit render provenance.

Source erasure removes the manifest, bundle registrations, completion certificate, document maps, staged
artifacts, source payload, and source-owned derived support through the lifecycle
transaction. Restoring older data remains subject to the independent erasure journal
and readiness barrier. These tables do not provide an alternative restoration or
source ordering authority.

The source tests cover partial fanout, immutable expected sets, model markers without
vectors, explicit zero-work plans, all-space restart discovery, historical mapping,
revocation/deletion races, and rejection of completion that omitted required artifact
transitions. They also cover failed/pending/reclaimed predecessor work, queue archival,
validated empty-plan receipts, explicit skip, and selected-span evidence bounds. Authored cases are not executed evidence; release qualification records
the actual integrated source, backend, artifact, and checks run.

## Recovery after a processing configuration change

A pending source can recover from a missing or revoked processor after an approved
configuration repair. Its accepted `PublicationFence`, source revision, and operation
remain unchanged. The original `SpaceConfigToken` identifies the processing plan;
`source_plan_id` combines it with the accepted fence. Every replacement work item and
model artifact has a configuration-specific identity. Work carries its original
`_source_configuration` and `_source_plan`; an older claim cannot adopt a newer plan.

`replace_operation_plan` compares the exact previous manifest digest, unchanged source
head, current approved configuration, original live sealing attempt, and absence of
completion inside one transaction. It archives the immutable old expectation set in
`source_publication_manifest_history` and replaces the active closed manifest. Metadata
and planning outputs must be newly produced under the new configuration. A retry cannot
reuse old model artifact IDs or turn incomplete old work into successful new work.
The new extraction fanout begins only after this replacement seal is durable. Ordinary
same-configuration retries still reuse the exact existing sealed set.

The finalizer seals its own original attempt into `SourcePublicationRequirements` before
receipt replay. Both backends check that claim's original plan payload against the active
manifest inside canonical publication. Candidate/planning context tags and producer work
payloads must agree; the checks also apply to retained queue archives. An unversioned
initial plan may still be read, but a replacement plan cannot be finalized by an old
unversioned claim. An already completed/published source revision cannot be replanned.

Credential rotation produces a new processing-work version but preserves the unchanged
`VectorPartition`, prior published graph, and valid vectors. A semantic embedding-profile
change requires its own real indexed coverage. Neither case removes the prior published
source while replacement work is incomplete. Immutable rendering/tree results can be
reused only when the recipe is unchanged; changing the rendering recipe needs an
explicitly accepted reprocessing revision. Retrieval must check current-manifest document
membership, because historical staging from another configuration can share the same
accepted external source revision.

A crash after replacement sealing but before child enqueue leaves the new closed set in
the all-space pending scan. Configuration drift before sealing leaves the old pending
manifest available for the runner to reconstruct a deterministic configuration-specific
chain under the same accepted source. The new migration 037 adds history separately from
the existing migration 035. History has explicit space/source ownership, participates in
canonical epoch and erased-reference checks, and is physically removed during source
erasure. It does not add a source-state authority.

Additional test source covers configuration recovery on both backends, stale digest and
attempt rejection, old-output reuse rejection, unchanged accepted/published authority,
original-finalizer fencing, immutable history, completed-source refusal, restart before
new child enqueue, and unchanged vector identity during credential rotation.

The erasure regression `actual_erasure_removes_current_and_superseded_plan_payloads`
starts with the real acceptance API, rotates a pending metadata plan, and invokes
`erase_live` with the captured authorized source generation. It inspects physical
inventory for both plans' producer, run, candidate and bundle records, checks
current manifest and history removal, and preserves an independent entity. It is
an authored dual-backend regression; execution is part of coordinated validation.

Source embedding writes capture `SourceEmbeddingWriteAuthority` before provider
work and recheck it in the vector repository's transaction. It binds the original
embedding claim, exact declared job and revision, current manifest/configuration,
source head and document mapping. The check also binds node content and its full
security/subject envelope; only the mutable embedding marker and update timestamp
may differ between batches. Expired workers, changed inputs/configuration and
completed publications cannot overwrite vectors. Batches may cover subsets of
planned nodes, but finalization still requires the complete declared set and the
successful original embedding work run. Empty batches also validate the claim.

Three additional dual-backend regressions exercise superseded-worker writes after a
successor commits, configuration/input changes during inference, and foreign
vector envelopes or undeclared nodes. They use the actual guarded vector write
API and assert stored vectors and node markers, rather than queue status alone.
These tests are authored; coordinated execution remains the validation gate.

The worker also calls `validate_embedding_input` on the owned node/revision
snapshots before constructing provider text. It feeds those same snapshots to the
provider. This rejects transient rereads even if the database returns to its old
value before the commit check; the write guard remains required afterward.

Completion is scoped to the full accepted `PublicationFence`, including its
generation and operation. A fresh acceptance may reuse an immutable revision
string already shown as published (for example, A → pending B → A). That string
equality does not complete the new operation or prevent configuration repair.
Only the exact acceptance's completion certificate prevents replanning it.

The accepted-instance replan regression also exercises the real acceptance and
canonical publication APIs: publish A, accept B without publishing it, then
accept the original immutable A under a fresh idempotency key and exact current
source generation. The old A receipt remains historical; the new A operation
has no completion despite equal desired/published revision strings. The test
rotates processing configuration, replaces the new instance's manifest, and
commits a distinct completion through its original finalizer attempt. This
covers the actual transition in addition to the narrower synthetic-head guard
fixture; it does not claim consumer or provider execution qualification.


## Accepted contact effects

Contact intake captures its authorized entity, identity-key and relationship
changes with the exact source acceptance. The source finalizer calls
`CanonicalRepository::append_accepted_source_effects` after preparing source
replacement and before extraction promotion reads the plan. It passes that same
plan to `prepare_assertion_groups_with_plan`, retaining its epoch, original
before-images, dependencies, effects and outbox. Promotion does not restart
retirement or overwrite contact effects after resolution. Its immutable support
and authorization partition checks still apply; an accepted contact alone does
not establish extraction grounding. All effects, artifact completion and the
Source projection join the original attempt, publication fence, receipt and
published-head transaction. Metadata-only and explicitly planned empty-group
publication retain the same plan; contact effects do not require an extraction
model.

The accepted attachment retains original before-images and exact identity/company
predicate matches. Storage rechecks them at acceptance and inside finalization.
A changed match set or authorization snapshot fails instead of rebuilding a plan
from current metadata. The finalizer's current canonical epoch detects concurrent
changes during preparation; it does not reuse the pre-acceptance epoch. An
operation retry or processing configuration replan uses the same attachment.
The attachment is private source-scoped data and is physically erased with its
source. The storage and intake owners supply admission/readset regression tests;
the finalizer hook alone is not a claim of tested end-to-end contact publication.

The worker library regression `source_contact_finalization_tests` exercises both native
and embedded storage with real typed contact preparation and acceptance, the
claimed work dispatcher, durable manifest, and finalizer. It requires no live
contact or extracted graph before finalization, then checks original-operation
completion, structured contact fields/identity/relationship, separately grounded
document evidence, source support and both kinds of outbox effects in the same
canonical receipt. A completed acceptance retry must preserve the exact graph
and outbox without further provider dispatch. Its extractor is a fixed independent
known-answer verifier and its embedding transport uses the existing loopback spy;
this is publication composition coverage, not model-quality qualification. The
test source is authored; execution requires the complete contact integration.
The source carries a non-default classification level, label and handling rule;
the contact, identity key, relationship, grounded graph, assertion and canonical
outbox must retain its full Security value. Resolved subjects may add restrictions
and are not required to equal the input's subject set.

The shared storage manifest fixture now publishes its prior metadata revision
through actual acceptance, claimed producer/finalizer work and canonical commit,
then accepts the pending replacement with the current generation and authorized
source snapshot. Its previous-publication checks therefore retain their original
meaning while every publication has a real immutable input and operation receipt.
The native fixture explicitly applies source, manifest, history and private-reference
schema boundaries in order; it does not reinstall an older source
migration over the current guards. Deliberate race/corruption tests still make
their named state changes after this valid baseline.

Four further native/embedded runtime definitions cover contact lifecycle inverse
semantics. One seeds an independent person and email identity, publishes contact
revision 1, publishes revision 2, and physically erases the source. The remaining
person/key must match the complete pre-source baseline (apart from update time),
including metadata, aliases, security and evidence; neither revision's contact
contribution may be restored. The other publishes, retracts, explicitly accepts
with `reactivate` and the retracted generation, then publishes through new claimed
work. Acceptance alone cannot restore graph visibility. Each replacement compares
all live graph rows with the prior graph immediately before the real finalizer.
These cases require the contact inverse in the existing source-decision receipt;
they do not bypass tombstones or invent a second restoration authority.

The same runtime fixture also publishes two separate contact sources with equal
security/subject envelopes, then calls the retained public `supersede_rel` path.
That path must accept the sealed structured-contact proof without fabricated
extraction evidence, change only the requested old relationship status, preserve
the replacement, and commit its audit through the canonical transaction. Repeating
the request must replay the same result without another audit/outbox write or
model request. This positive coverage depends on the maintenance owner's accepted
contact support validation, alongside the owner's negative shape/provenance tests.

The lifecycle oracle also checks physical contact-person/key absence immediately
after retraction, with positive physical key presence before the mutation, and
reuses the deterministic person identity on explicit restoration. Replacement
revision 2 must already omit revision 1's alias while retaining the independent
original aliases. The supersession case checks both actual published heads and
full matching authorization envelopes before calling the public operation. The
worker package exposes `main.rs` through `lib.rs`, so these runtime definitions
are included by the workspace library test target after their module joins.

Expected contact baseline and supersession values are compared independently as
complete serialized JSON values, removing only the top-level `updated_at` clock.
The oracle does not call the product's support fingerprint, so changes to its
normalization cannot hide lost nested provenance, authorization or contact fields.

The finalizer boxes the readiness, retirement, accepted-contact, shared promotion
and canonical transaction futures at their sequential phase boundaries. This
limits the amount of nested future state carried by the work dispatcher without
changing its original attempt or transaction ordering. An unpolled actual-future
size regression sets a 64 KiB dispatcher contribution budget; it makes no
publication or provider request and does not substitute for runtime stack tests.

The actual A→pending B→new A oracle requires the first canonical receipt to
include its Source projection while recording no derived support for that Source
itself. Acceptance owns the mutable input through its head and immutable revision;
its next accepted state cannot invalidate a stale proof of self-derived output.
Derived graph support retains the existing exact fingerprint requirements.

The contact composition fixtures also box their storage setup, intake,
acceptance, queue, dispatcher, lifecycle and supersession phase futures. Central
runtime 2 reached all eight cases but aborted with stack overflows before they
could establish their invariants; this source repair does not claim those cases
passed. A separate type-size oracle checks the four actual scenario futures
against 64 KiB without constructing or polling them. It catches fixture state
aggregation before an outer allocation can hide it. The runtime keeps its
normal stack limit, and the existing publication/physical-removal assertions are
unchanged. Central execution remains required after this checkpoint.

Two additional retained-path oracles publish a plain Metadata source that carries
contact-shaped metadata, then create and claim a real reprocess coordinator with
its persisted original WorkRun/attempt. Admission and pending retry must retain
one successor operation and the original plain capability class. Actual source
children and the finalizer complete that successor before reprocess accounting
may mark its job complete. The fixtures require a current MetadataReady source,
canonical completion certificate, unchanged contact input, zero contact graph
rows/outbox and zero embedding transport calls. They target the coherent private
producer-link group and the maintenance owner's explicit plain-contact retry
repair; they have not been executed at this source checkpoint.

Readiness distinguishes a declared bundle that has not arrived
(`source_publication_incomplete:expected_bundle_missing`) from a persisted
bundle lacking valid validation state (`bundle_not_validated`) on both storage
backends. Both leave the published head and completion certificate unchanged.
An early finalizer notification is not publication: scheduling must retain the
closed requirements and retry only through the durable work lifecycle while
unfinished extraction, registration or embedding predecessors complete. The
storage parity regression isolates absent bundles, absent validation time,
Draft status and recorded validation errors; it is a storage fixture, not an
extraction-dispatch qualification.

Bundle registration boxes its sequential manifest lookup, candidate validation
and durable registration phases, as the finalizer already does for preparation
and commit. The type-size regression inspects both returned future types without
constructing or polling them; this avoids a size test itself exhausting the
stack before its assertion. It does not establish that every dispatcher phase
fits its runtime stack; the actual contact scenarios remain required.
Six further source-only contact-lineage scenarios use actual independently
accepted A/B contact publications. They cover erasing B and restoring A's entire
original support ledger, retaining a real public IndependentMutation status edit
through B erasure, and erasing A while independently admitted B survives. Each
case compares the complete surviving Rel to an independent expected value,
checks physical prepresence/removal of raw canonical receipt payloads, and then
performs actual public supersession against a third published contact. The first
case requires A's exact original transaction pointer; the latter cases require a
physically stored surviving-only continuation with a distinct actual identity.
Public receipt serialization must omit private lineage. These scenarios depend
on the coherent integrity/erasure/maintenance lineage group and have not yet been
executed; they do not manufacture completed heads, support rows or receipts.

The erase-A/survive-B cases now continue after physical removal of B's historical
prepared contact plan (which contained A before-images). The original exact
admission descriptor must survive unchanged. A matching historical request uses
the shared trusted storage intake adapter and returns only the original receipt,
with its actual status and no processing requirement. Complete source, document,
graph, outbox, queue and artifact snapshots must remain unchanged; the erased
plan stays physically absent. This is storage composition coverage. Separate
intake-owned MCP tests cover actual public source/document scopes and ABAC.

The same actual survivor then enters a newly created, claimed reprocess job with
its persisted original WorkRun/AttemptFence. A fresh contact plan is required
under a new source operation, with the old published graph retained while pending.
The actual Graph worker fanout and finalizer complete that operation, retaining
the source authorization envelope and proving contact support before subsequent
public supersession. The original erased plan and raw A/B historical receipts
must remain absent throughout. No positive head, support, completion or contact
capability is fabricated. The existing plain Metadata reprocess cases reuse the
same real coordinator and operation-driven dispatch helper. All new source is
held for the complete admission/lineage/intake/maintenance group; no local Cargo
or runtime execution has been performed for it.

The fixture also includes the independent qualification-owned child at
`tests/recall_preview/contact_lineage_oracles.rs`. Its fault setup and expectations
remain independently owned; the child reuses the real publisher rather than a
second synthetic authority implementation.

The surviving-plan oracle also requires the strict historical-plan getter to
return the same typed `accepted_contact_plan_unavailable` conflict as contact
preparation. It rereads the persisted original operation after both retry entry
points; returning a cached correct receipt cannot hide an operation mutation.

Two additional backend variants test distinct surviving fields without changing
the identical-fact cases. A actually publishes Casey, a shared email and an
A-only profile; B independently publishes Blair, that same email and a B-only
profile through the same selected physical person. Before erasure the fixture
checks Casey's display, Blair's admitted alias, both profiles and the A-created
contact metadata. After erasing A, the same person must display Blair, retain
only B's email/profile keys and security/provenance, and contain no A display
alias or A-created metadata. The A-only key must be physically absent, and both
surviving keys must cite only B as evidence. The original B plan remains absent
through the same read-only retry, fresh actual Graph reprocess and supersession.
Expected values are explicit fixture facts, never read from the product's
contribution projector. Arbitrary inherited aliases and independent later field
edits are covered separately by the projector/inverse owners' pure tests.

The actual contact oracles now respect durable PendingDependencies retries. The
shared dispatcher fixture persists the original unsuccessful WorkRun and refunds
only the dependency retry through the production queue transition. Contact
oracles reject every other error, verify the persisted original attempt and
released lease, require the graph to remain unchanged, and count only successful
finalizer completions. Because runnable queries exclude future `run_after`, a
bounded full fixture-queue inspection distinguishes delayed work from missing
fanout and waits for the real deadline without rewriting it. Each publication
has a120-second wall-clock bound in addition to its dispatch-count bound. The
fixture config selects the supported30-second retry cap; the production policy
still imposes its actual minimum30-second dependency delay. Independent original
contact inputs also use distinct stable rendition keys derived from their
original Source IDs, preserving the native per-space rendition uniqueness.
Actual pipeline execution remains centrally scheduled.

Embedded physical control inspection uses the actual table-qualified extension
row key and advances the kernel's101-row pages up to the explicit256-row fixture
bound. An empty lookup in a truncated first page cannot establish physical
absence. Native inspection still uses the exact typed table/key identity.

Standing Watch delegation is checked at every manifest write boundary: initial
closed-plan publication, validated-bundle registration, original planner restart
adoption, and unpublished configuration replanning. Each operation resolves B's
prepared authority from the original source operation and checks that captured
proof inside the same embedded or native transaction. The vector authority also
captures this proof before provider dispatch and retains it through the actual
vector write; a completed provider call cannot refresh a revoked grant. A owns
the corresponding canonical finalizer check and I owns document staging. The
manifest and completion remain the original publication records, with no new
delegation state in either public type.

Missing original producer evidence cannot be treated as unrestricted direct
admission. New both-backend fault tests remove only that field from a real
accepted operation, verify unchanged head/manifest/claim and refused manifest
retry and embedding preparation, restore exactly the original control, and retry
the same operation and claim successfully. These tests do not create a trusted
Watch descriptor. Actual delegation revocation, descendant ancestry, archival,
and concurrent grant-change cases require the composed B/A/I/F/D standing-watch
group. This source checkpoint depends on B's sole prepared-authority contract;
no compiler or runtime validation has been performed in this worktree.

Native finalization guards use actual writes to the private shared reference
revision after checking the original source/configuration and input rows.
The pinned native engine optimizes away same-value updates, so rewriting a
payload or timestamp to itself cannot establish transaction conflict. Migration
039 and the additional mapping/registration writer events in 043 cover changes,
deletion and movement of present rows from their original space. Head and
optional-manifest reads also use the bounded physical-key fence from 043,
including an absent manifest observed before bundle registration. Original
payloads, timestamps, artifact requirements and attempt comparisons remain
unchanged; an actual successor-manifest write still writes its new payload.

A native source regression reuses the original captured embedding authority
twice and checks that the private reference revision advances while Source,
Doc, DocRevision, nodes, artifacts, queue claim, head and manifest remain
unchanged, without completing the source. Deterministic overlapping-writer
qualification remains an independent test requirement.

The durable extraction-plan checker recognizes the same configuration-scoped
bundle IDs selected by active fanout. Original pre-versioned manifests retain
their explicitly declared IDs. Both formats still require the exact complete
selection set, including revision, representative node, all coalesced nodes and
byte spans. A source-level regression covers both storage backends, restart
lookup and idempotent sealing, plus altered configuration IDs, missing or
duplicate selections, and changed spans or membership. Sealing does not create
the declared bundles or move the published source head.


Standing authorization now includes the exact persisted inputs consumed by the
closed publication plan. `declared_input_records` point-reads its documents,
revisions, representative and coalesced nodes, embedding nodes, metadata and
planning artifacts, and materialized knowledge bundles. Every materialized
bundle also contributes its original candidate. Expected future knowledge
bundles may be absent; that never proves readiness or completion. Missing or
deleted required inputs refuse. The caller retains its original desired head,
manifest presence, membership epoch and document observations.

The reader deduplicates exact identities and refuses inconsistent repeated
observations. Its checked upper bound comes from the original manifest:
`2 * documents + embeddings + planning artifacts + 2 * metadata artifacts +
2 * bundles + sum(bundle.node_ids.len() + 1)`. This includes each possible
original candidate and representative node without a separate arbitrary row
limit or a truncated prefix. It is a bound on selected records, not a measured
large-document performance qualification.

The five manifest/vector write boundaries authorize these original input rows
through B's captured standing authority and I's sole record-to-scope mapper.
Every inherited principal applies. Embedding snapshots retain complete revision
and node policy fields. Only a node's own mutable embedding marker and update
time are normalized between vector batches; the revision normalizes only its
update time. Both native and embedded vector commits compare those original
inputs again. Proposed outputs do not become input authority.

The source tests cover exact membership before and after one bundle arrives,
original candidate deletion, missing required artifacts, coalesced-node
deduplication, explicit Metadata/Retrieval populations, and revision policy or
renderer changes before vector commit. They use actual accepted source fixtures
and make no provider, standing-Watch concurrency, or completion claim. These
Rust tests have been authored but not executed in this worktree. Before-manifest
provider phases still require their actual inputs: the retained optional tree
summarizer consumes a DocRevision section before sealing, so Source/Doc-only
capture is not a universal dispatch proof.
