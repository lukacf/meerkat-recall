# Atomic truth maintenance and resolution

Truth maintenance prepares a complete `CanonicalPlan` before changing the live
store. The plan includes claims, truth slots, conflict groups, predicate
proposals, audit rows and semantic outbox events. The storage-owned
`CanonicalRepository` commits these effects and their receipt in one transaction.
The resolution patch compatibility path includes every explicitly selected
artifact status in the same batch transaction. An existing claim or committed-looking artifact does not
substitute for a complete matching receipt.

Preparation captures the space's canonical epoch before any decision reads.
The planning adapter retains exact original rows, overlays planned updates and
coalesces repeated writes to the same row. It performs no live writes. An error
in a later claim, invalid evidence, or an invalidation scan discards the entire
plan. Event and truth-slot scans use cursor pagination; they do not treat a
capped list as a complete conflict population.

The final transaction compares the epoch and read set and validates the original
background work attempt. Configuration changes, source lifecycle changes,
competing canonical writes and expired/replaced attempts invalidate preparation.
Deferred adjudication and background resolution patches also carry the epoch
captured before their input reads and provider call. Artifact staging does not
advance this semantic epoch; exact artifact snapshots remain transaction guards. A stale result must be prepared again; it cannot mark a conflict
resolved or populate a successful adjudication memo.

Maintenance of published knowledge uses accepted immutable support. It captures
the current source generation and the exact already-published revision from the
support ledger. A newer desired revision is not an authority to rewrite older
evidence. Evidence checks include the assertion, immutable revision, UTF-8 span,
quote hash, exact immutable document/revision mapping, accepted manual payload
text, optional node bounds and complete authorization envelope. Maintenance does not finalize a new source operation or invent a source
completion proof. Independently authored source-less records retain their
separate authority.

Claims compare and corroborate across sources only within the same security and
subject envelope. Every contributing assertion retains its exact accepted source
revision. Derived rows explicitly carry the union of their decision inputs;
canonical storage records causal inverses in the same transaction. A decision
requires every contributing source. Retraction or erasure invalidates it and
its dependent decisions before pruning independent raw support. This restores
surviving claims closed solely by removed evidence, including supersession dates,
without overwriting later independent changes. Sharing an entity alone never
grants another source's evidence authority. The storage owner implements the
inverse/erasure transaction; this module supplies exact original and final rows.

Adjudication returns proposed predicate semantics with its commit input. It does
not persist proposals or successful memoization before claim application. The
commit compares the original semantic fingerprint, then applies the proposal
and claim changes together. Operator-configured semantics remain authoritative. Learned proposals inherit
exact input support and cannot overwrite a proposal owned by another partition.
Such proposals apply locally, with an explicit scoped rationale and a memo bound
to the selected local semantics. Future resolution conservatively re-adjudicates
until a typed durable scoped semantics field is available.

Idempotency identifies the semantic request separately from generated row IDs,
timestamps and lease tokens. Retrying an operation reuses its completed receipt;
reusing the same operation identity with a different request conflicts. Durable
replay also retains the pending successor output, so interruption between commit
and enqueue does not lose conflict-resolution or direct-commit work.

The deterministic regression source covers preparation without writes, repeated
overlay updates, late transaction failure, complete receipt replay, changed
payload conflicts, stale configuration/source/attempt inputs and independent
support preservation. Tests exercise the real embedded and in-memory SurrealDB
transaction paths where applicable. Authored tests are not execution evidence;
qualification must name the integrated source revision and executed backend.

Integration dependencies: integrity canonical commit/receipt replay and causal
inverse metadata; finalization immutable document mappings; recovery original
attempt dispatch; providers approved binding-version getter. Tests here include
retained no-head source fixtures explicitly guarded by exact Source snapshots.
Those fixtures are not proof of newly accepted source publication or physical
erasure. Qualification must also execute the real published graph-source and
source A / source B erasure oracle after all owner commits are joined.

The work-run compatibility wrapper selects the currently validated artifact IDs
and commits that explicit set. It does not certify a closed whole-run or
whole-source manifest. The source finalizer owns those completeness guarantees.

Read support is a certificate for the complete stored domain record, excluding
only `updated_at`. All ledger entries must match that fingerprint before any
projection. Independent baseline authority cannot preserve an unsanitized source
contribution: reads hide such aggregates until canonical lifecycle reconciliation
has removed the invalid contribution and refreshed its support. Source-less
intrinsic records keep their separate authority.

TM and retrieval accept coalesced evidence only within the exact persisted
publication manifest bundle selected for that physical source revision and
anchor node. This may extend past the first node's own range. The quote and window
must stay within UTF-8 bounds; unrelated anchors and revision-wide widening are
rejected. Legacy/no-manifest evidence retains the physical node bounds. The
shared range helper grants no authorization or semantic content proof by itself.

The read-proof regression source includes full-record tampering, stale shared
support, source-less reads, aggregate reference loss, exact coalesced windows,
and retained physical-node bounds. These tests require integrated validation;
source authorship alone is not a passing qualification result.

Deterministic decisions found during active conflict detection are applied to the
same overlay before its single commit. Returned successor work contains only
conflicts that need deferred resolution; deterministic payloads are not queued
with an epoch made stale by their own detection transaction.

Wiki read validation treats only the declared article writer/verifier model and
prompt dependencies as descriptive compiler inputs. Graph and context dependencies
load their real records, check observed versions and authorization, and traverse
nested immutable revisions with cycle/depth/work bounds. Descriptors cannot grant
evidence authority. A verified document node may receive safe text projection from
its immutable revision; changed semantic projections remain ineligible.

Retained public supersession helpers now read both the old record and its actual
replacement before staging any effect. Revision links, lifecycle changes, audits,
outbox notifications and replay output commit together. A missing replacement
leaves the old row unchanged; replaying the same old/replacement pair returns the
original result, and reusing the old record's operation for a different replacement
conflicts. These helpers do not claim that the larger reprocess job's extraction
and progress stages are already atomic; that adapter is a separate ongoing join.

When the resolver proves its existing semantic memo is current, it may acknowledge
a stale slot in the same canonical transaction without reapplying a verdict.
The acknowledgement preserves claim status, evidence and confidence, and writes
only the slot freshness change plus audit/outbox/receipt. This lets explicit
re-maintenance converge without turning a memo's sentinel response into a new
NeedsReview decision.

Evidence authority reuses the storage owner’s envelope inheritance: one opaque policy domain, no lower sensitivity, all contributor labels/caveats/subjects retained and no more permissive completeness. Ordinary mutations keep an exact partition; explicit subject-only staging keeps security and completeness exact. A separate context-inheritance operation accepts stricter envelopes only from actual observed inputs and retains their source ownership. Published coalesced windows additionally require the completion certificate to match the current manifest digest, so historical plans cannot certify current evidence after a configuration replan.

Reprocess jobs now separate accepted source replacement from maintenance effects.
ReRender, ReChunk and ReExtract freeze the complete selected set in one canonical
transaction. The coordinator retains a digest and count; each immutable source
intent is stored under its source security/subject/support envelope and references
the original job run. Its body and envelope are included in the digest. Batch and
daily limits pace admissions rather than truncating selection. Acceptance uses the
original successful attempt and captured space configuration. Original operation
IDs survive retries; missing source intents are never reconstructed after erasure.

A replacement is complete only when the source owner reports Completed and the
original publication fence has a certificate matching the current manifest digest.
Explicit approved configuration replans use distinct child plans; neither pending
acceptance nor an old plan certificate counts as completion. ReMaintain and
ReResolve instead share one preparation overlay with their job state, once-only
seed, audit/outbox and receipt. Deferred conflict work keeps the job pending and
recompute does not re-dirty completed children on every poll. ReResolve reuses
actual canonical assertion evidence through the grounding owner's preparation
helper; it does not create a new extraction receipt from an existing assertion.
Predicate selection accepts an exact resolved predicate ID or a raw alias using
the alias repository's ASCII case-insensitive lookup semantics. Extractor and
source/date restrictions still apply together; canonical IDs remain case-sensitive.

The source includes repository-backed intent/retry/admission/erasure/stale-config
and maintenance replay/rollback tests. They have not been executed by this worker.
Source replacement results now include a read-only usage snapshot of their
original accepted operation IDs across attempts and configuration plans. The
recovery owner's actual receipt projection supplies physical dispatch and token
counts. Unknown monetary evidence remains `None`, including when other operations
are known zero. Completed-job reads refresh this projection; it is not a new
billing ledger or source completion authority. Current physical receipts lack
pricing evidence for model calls. Explicit finite-budget source replacement and
ReResolve jobs therefore pause (or fail under the existing option) before new
acceptance, provider preparation, or child handoff. Restored selections take the
same restriction. The job stores the refusal reason; its legacy
`cost_spent_euros` field is not measured source-child spending.

Fresh deterministic ReMaintain remains available with a finite budget. Its
preparation uses an independent branch at the original epoch and attempt. If it
requires adjudication, that branch is discarded and only the budget refusal is
committed, without seed, graph effects or provider-child IDs. A previously started
finite maintenance job with unverified prior monetary usage also pauses. Jobs
without an explicit limit retain their existing behavior. This preserves the
retained stop-after-recorded-spend contract, without claiming a priced hard cap.
The physical-dispatch owner must additionally guard already accepted work and
retained children using their original durable job linkage; coordinator admission
alone cannot enforce that boundary. The broad maintenance adapter follows.

`reprocess::guard_physical_dispatch` reads the actual work item and the recovery
owner's private immutable producer origin. Child origins bind the original
parent, actual run and attempt, plus both work fingerprints. The bounded chain
may cross public authorization partitions; it only adds accounting restrictions
and never grants permissions. Unknown or withdrawn origin evidence refuses
dispatch. Recovery installs this check before creating a physical-call receipt.

Source dispatch reads the private producer attachment on the original accepted
operation. Reprocess origins must still match the actual job, original selection
digest, complete intent membership, exact target fingerprint, authorization,
immutable input and all four publication-fence fields. Explicit direct intake
stays independent of other jobs' controls. Deleting every selection control cannot
turn a retained reprocess origin into direct intake. Existing receipt recovery
also verifies the original producer, so an identical public idempotency key with
a different origin is a conflict.

The original job restriction is captured in the job-creation transaction, before
a queue handoff or claim is required. An original finite budget or a current
finite budget blocks unpriced physical dispatch; changing a formerly finite job
to `None` cannot authorize it. A distinct fresh unlimited job remains available.
Source acceptance receives every selected intent snapshot and its closed-set
predicate, under the epoch captured before reading the selection. The owner
checks this full read set in the acceptance transaction. Saved contact input uses
the shared intake service-authority validator.

Source oracles cover original finite-to-None refusal, a fresh unrestricted job,
removed complete selection controls, independent direct intake, and private
child dispatch across different public policies. They invoke real repository,
claim, run and child-enqueue paths without a provider call. This source checkpoint
requires the complete A/B/C/E producer group and central validation; it does not
claim a measured monetary hard cap or reconstruct missing historical lineage.

Re-resolution validates dependency and original before-image claims against
original captured assertions, and new effects against prepared assertions. This
preserves original source checks while requiring newly added subject restrictions
on the new graph. Event support uses the same distinction and validates each
assertion's immutable evidence chain directly.

Structured contact relationships with no document assertion use the canonical
owner's accepted-contact proof for every physical source instead. The overlay
observes the actual original relationship and Source snapshots before checking
the sealed original contribution, current published support and completion
receipt. Status, bounded confidence and tombstone changes retain factual identity,
endpoints, predicate, validity and exact source contribution; security may tighten.
Arbitrary editable contact metadata and shared entities do not provide proof.
New factual IDs, redirects or cross-source contact merges need additional owner
lineage authority and cannot borrow a different contact's certificate. Existing
assertion-backed relationships keep their immutable evidence checks. This branch
requires the owner's contact-publication group; its positive published workflow
oracle is coordinated with finalization, and is not claimed executed here.

Source reprocessing of a structured contact freezes the actual original accepted
capability fingerprint with the immutable input and selection. After that selection
commits, a fresh contact plan is prepared from the saved input, with its own current
epoch and exact predicates. Every old and proposed record must fit the original
authorized source envelope. Acceptance commits the new seal under the original
attempt and configuration; a retry recovers the already accepted seal before any
selector reads. Editable metadata cannot supply the original capability, and an
old plan epoch is never refreshed to bypass the source owner's guards. Ordinary
non-contact recipe identities retain their prior encoding.

Opt-in historical classification repair has its own narrow staging operation.
Only a baseline envelope can change; semantic fields, provenance and source
ownership remain identical. Completeness can only become more restrictive under
the storage owner’s envelope inheritance rule. Actual contributing documents determine
one compatible policy, sensitivity maximum and required-label/caveat unions.
The complete supplied document set must match the real evidence revision chains.
Original before-images remain transaction inputs even when their validated repair
envelope is used to check previously missing restrictions. Mixed policies cannot
be resolved by comparing opaque policy identifiers. The same storage-owned inheritance rule checks each contributor of a combined
envelope; mixed policy domains remain invalid. Runtime qualification is pending.

Discovery scans may inspect the prepared overlay under the original epoch without
making every unrelated row a causal input. Selected pair, dossier, evidence and
changed-row snapshots still enter the exact read/effect set. Storage-owner plan
adoption must preserve every original dependency, before-image and notification.

Only TM's own verdict/acknowledgement notifications suppress recursive claim
processing. Re-resolution, supersession and hygiene notifications reach normal
TM detection, including new claim slots that did not exist during recomputation.


## Retained broad maintenance sweep

The periodic `run_maintenance_sweep` now prepares entity hygiene, relationship and
assertion deduplication, identity decisions, entity type consolidation, predicate
registry discovery, and predicate hygiene in one original canonical epoch. The
configured extension reference registry installs its epoch guards before capture.
Queue callers can use `run_maintenance_sweep_with_attempt` with the original claim
and actual WorkRun; the retained periodic standalone call supplies neither.

Classification repair remains opt-in. When enabled it prepares first, so later
phases consume the repaired inherited envelope in the same overlay. Any semantic
preparation failure aborts the entire graph change. The final canonical transaction
contains all effects, complete read dependencies, inherited audits, outbox records,
and the durable result receipt. No earlier phase commits independently. Existing
identity/type candidate limits and predicate scan limits remain explicit bounds;
classification reports its cap rather than claiming the whole space was repaired.

Hygiene preserves old relationship tombstones and creates replacements atomically.
Redirects retain original subject restrictions and add survivor restrictions.
Deduplication partitions by exact security, subject membership, and completeness;
its retained winner is an actual decision dependency. New predicate registry names
inherit an actual graph contributor rather than acquiring default public authority.
A unique registry name already owned by another partition is not broadened.

Predicate preparation preserves syntax aliases, explicit opt-in automatic aliases,
and default review proposals. Every draft points to a real WorkRun and WorkItem.
Standalone proposals use the queue owner's deterministic allocation-only context,
created only when drafts exist. That terminal pair makes no execution, publication,
or model usage claim. An orphan pair after a later canonical failure is allocation
metadata. Both actual rows are exact read dependencies, including across archive
movement; canonical effects cannot mutate queue records. Changed or removed pair
snapshots fail the proposal transaction.

Entity vector-index refresh and failed-work recovery continue after the semantic
transaction under their own owners. Their outcomes are not certified by the graph
receipt. Paid calls and monetary accounting remain separately governed; this source
checkpoint does not qualify an end-to-end monetary budget.

New unexecuted tests cover a late real transaction fault across hygiene, dedupe,
registry, audits and outbox on both storage backends; exact security/subject
partitions; provider failure after syntax preparation; and changed/deleted actual
proposal work dependencies. Retained fixtures now use the canonical schema and
space configuration, and the evidence-union test persists its actual assertions.
Only direct rustfmt and static diff checks have run in this worktree.


## Public reprocess job allocation

The MCP create handler requires `write:reprocess_job` and authorizes the complete
job base envelope before allocating anything. It calls the queue owner's
`ReprocessJobRepository::create_with_work`, which commits the Pending job and its
deterministic initial work item together. Malformed or reversed date filters are
rejected rather than silently turning into a wider selection. Existing job status
and progress surfaces remain available. Authored tests cover the real queue
handoff, missing write scope, subject-restricted principals, and invalid dates;
none have been executed locally. The constructor depends on recovery's atomic
handoff and lifecycle's `HasSecurity` implementation for `BaseFields`.


## Configured predicate authority

`Configured` is a semantic status, not permission to ignore a predicate record's
security or source ownership. TM preserves the retained explicit global operator
case only for the built-in literal `default` policy, level zero, no labels,
handling restrictions, subjects, or source ownership. Its historical empty
subjects with `Unknown` completeness remain compatible. `Partial` completeness
never grants the global exception; it requires the exact input partition. Empty or arbitrary named
policy IDs do not establish this public default. Source support is checked against
the canonical source ledger as well as provenance; clearing a domain row's source
list cannot make a supported policy global. Other configured/learned predicates
require the exact live space, security/subject partition and source set of the
actual input. Alias version metadata uses the same scope restriction.

This is the retained built-in default envelope contract, not an interpretation of
arbitrary `PolicyDefinition` ordering. TM has no dynamic policy-registry authority
and does not infer that a named policy is public. The shared semantic fingerprint
format is unchanged; it now receives only applicable predicate semantics.

Authored tests cover retained source-less operator defaults, classified/named/
subject/source/foreign-space/inactive policy rejection, exact restricted matches,
and alias metadata isolation. A real canonical source-supported policy is then
deliberately corrupted to look public in the two-backend ledger oracle; the source
owner's existing support still prevents global use. These tests remain unexecuted.

### Runtime fixture and governance follow-up

Missing-semantics governance is now prepared by the TM commit phase and written
with the affected conflict group, claims, slot, audit and receipt. Resolving a
verdict alone emits no live event. A late canonical transaction failure rolls
back that governance event as well as the verdict; memo replay retains the
existing outcome.

The memo oracle uses two real independently authored assertion spans in a real
document revision. Empty-evidence candidates intentionally fail promotion and
cannot stand in for a committed verdict. Supersession and wiki fixtures register
a real named space; configuration race tests use the supported space lookup and
compare actual persisted snapshots. These source changes were formatted and
statically reviewed; central runtime validation is still required.

Policy scope checks consult the storage owner's causal-support lookup, including
legacy derived decision receipts as well as publication effect support. Clearing
a predicate row's visible provenance cannot turn a retained source-dependent
decision into independent global configuration. The lookup remains conservative
policy classification, not proof that a record is currently publishable.

Selection and admission may occur under different coordinator attempts. Recovery
keeps the original saved selection while a later source acceptance retains its
own actual admission attempt. Dispatch matches that saved fence to its real
retained WorkRun, not the older selection artifact's run or the current lease.
The coordinator also rejects a supplied run from a different attempt. The paced
two-source oracle exercises selection in one claim and admission in a later
claim; it remains source-only pending central validation.

An actual original operation accepted as plain input remains plain during
reprocessing, even when its metadata contains a contact-shaped object. The
source owner must first verify the exact original fence and return no accepted
contact capability. Metadata without that original authority still refuses.
Retry checks the accepted operation's original plain/sealed class before fresh
selectors; a plain retry never prepares a sealed contact plan. The boundary
oracle uses real admissions and verifies no entity or relationship is created;
full published-source lifecycle qualification remains a separate finalization
oracle.

Budget refusal uses the canonical owner's restricted `ControlOnly` context for
one existing reprocess job and its matching audit and outbox. It captures the
actual current source/head even when the source has no published revision. The
original epoch and attempt still fence the transaction. This path cannot mutate
graph records, source support, job selection, budget or authorization. Normal
maintenance continues to require published evidence authority. The restored
selection regression checks a pending source remains unpublished while the job
pause and exact audit commit together, without manufacturing support.

The retained `commit_artifacts` resolution bundle path uses
`ResolutionPatchWorkflow::commit_resolution_bundle_batch`. It checks the actual
validated source patch, exact payload, matching run and authorization/support
partition, and original decision epoch for every bundle before committing the
whole batch. Both source and bundle artifact statuses, assertion effects,
propagation and receipt share the canonical transaction. Original work runs are
captured transaction dependencies, and dispatch binds the current original
attempt with `with_attempt`. A changed payload cannot reuse a prior receipt;
ordinary queue staging does not authorize recapturing an old decision epoch.
The bundle regression sources cover success/retry, changed payload, incomplete
batches, stale configuration, missing epochs and mismatched runs on both backends.
These tests await centrally scheduled execution.

Wiki dependency reads use the domain `HasSecurity` subject view only for an
exactly captured graph root, including an entity's implicit self subject and a
relationship's endpoints. Raw record equality, support fingerprints and observed
versions remain checked; supporting sources and parents are authorized against
their own envelopes. This keeps intrinsic graph projections compatible without
normalizing Unknown/Partial parent scopes or inventing source publication.

Resolution patch staging inherits the actual assertion and original work-run
envelopes through the storage owner's conjunction helper before saving any
payload. Bundle commit rechecks that the artifact still contains both input
scopes, including physical source ownership; mismatched policy domains refuse
staging instead of creating a publicly scoped patch.

The retained disambiguator consumes a successful terminal provider outcome before parsing any candidate or executing returned tools. Terminal provider errors and truncated streams remain errors, including after partial text; they cannot become abstentions. The direct stream regression covers terminal errors, transport errors, truncated output and successful completion without making provider calls.

Re-resolution validates exact retired Rel, Attribute and Event after-images against their captured original assertions. This applies only when the complete original payload, security and support remain identical apart from Superseded status and deletion/update timestamps. New claims and any other changed payload still require the prepared assertion authority. The original snapshots and all new effects remain fenced by the same canonical epoch/read set and transaction.

Queued whole-sweep preparation binds the actual WorkRun to the complete original AttemptFence stored in its metrics, not just a shared work-item ID. It captures that run in the canonical read set, so a changed run after preparation prevents all graph effects. The both-backend source oracle exercises missing/foreign original fences, a concurrent run change with no graph write, and a valid actual queued sweep.

Retained TM detection, deterministic application, deferred resolution and recompute allocate their nested phase futures separately from the caller runtime stack. This changes allocation boundaries only; phase order, original epoch/attempt, scopes and the single transaction remain unchanged. A concrete repository/overlay future-size oracle accompanies the existing runtime and rollback cases; it does not substitute for executing those cases or increase worker thread stack size.

Watch-produced source dispatch verifies the storage owner's original Watch
producer against its actual work item and work run, then follows the same
private ancestor chain as other provider work. Completion of the old dispatch
lease or a later Watch observation does not rewrite that ancestry. Original
finite restrictions remain binding after a mutable job limit becomes `None`.
Source-to-work traversal shares active cycle detection, visited history and a
256-step total bound across recursive calls; it never restarts the accounting
walk at a source boundary. The Watch admission/budget oracle uses real queue
claims and storage acceptance, without fetching a URI or invoking a provider.

Fresh contact reprocessing captures the storage owner's minimal original
admission capability digest, so surviving source input does not need an erased
historical contact plan. It still prepares and authorizes a fresh plan from the
surviving immutable input under a new explicit source intent and current CAS.
Matching old operations whose original plans were erased are receipt-only:
pending or retryable states return `accepted_contact_plan_unavailable` and cannot
restage, enqueue or manufacture completion. Earlier saved selections retain
their original full-seal recipe encoding and require that actual seal; no digest
is synthesized from missing history.

Evidence-less structured-contact dedupe preserves the exact union of physical
source contributors inside one authorization and validity partition. Preparation
proves each original relationship through the canonical owner's current accepted
contact receipt. Redirects use only original captured Entity snapshots with real
merged redirects, never a guessed same-name match or newly staged Entity image.
The canonical owner keeps original receipt/support pointers for later proof and
erasure continuation; this adapter creates no separate publication authority.
Actual full-publication positives and erase-one-source continuation are owned by
finalization/qualification and require the coherent storage lineage group.

Published maintenance evidence uses the shared current-revision reader's original
completed render proof. Raw accepted templates and raw documents may differ from
normalized rendered text; neither trimming nor equal hashes substitute for the
completed manifest, original canonical receipt and exact rendered fingerprints.
The reader must return the exact Doc and current Source already captured in the
maintenance read set, under the assertion's full authorization. Published mapping,
source-content binding, physical source equality, UTF-8 span/node/hash checks and
original canonical epoch remain required. Source-less and explicit no-head legacy
evidence retain their existing separate contracts.

The actual event/timeline and truth-maintenance E2E scenarios exercise this
consumer after real ingestion and finalization; Runtime11 demonstrated the old
raw-versus-rendered comparison failure. This repair depends on the completed-render
proof source checkpoint and has not been executed locally. The independent
cross-source identity-merge provenance failure is tracked with the merge owner;
this change does not relax the assertion's physical evidence-source guard.

Generated Wiki definitions use the same maintenance transaction. Preparation
captures the epoch before inventory and provider calls; the complete actual
inventory includes disconnected entities used by navigation and all contributing
governed claims. The original stored rows remain the read set. Typed security
and subjects are joined through the storage owner's definition-envelope helper;
physical source provenance is the exact contributor union. A previous definition
is only a complete CAS/inverse before-image, never authority for new content.
Regeneration preserves identity and birth metadata and refuses tombstone revival.

Every definition in one exact security partition, its audit, outbox and durable
compilation jobs commit together. A versioned provenance pointer addresses that
actual receipt; the pointer alone cannot authorize a read. The receipt's exact
output and original material inputs must still pass the storage owner's current
proof. A retry replays the committed plan before another model call and rejects
changed semantic input. Unchanged article semantics preserve their definition
update timestamp so the compiler can validate and reuse a current article.

Different security partitions deliberately use separate transactions. Aggregate
results expose each successful definition receipt, partition failures, and later
withdrawal errors. Failure of one partition cannot hide prior articles in other
scopes. A recurring worker must save its completed graph fingerprint only when
article, partition and reconciliation errors are all absent. This is not a claim
that a multi-partition request is one atomic transaction.

Withdrawal stages the complete obsolete-definition set before one canonical
commit. Only publication status becoming Hidden and updated_at may change;
original content and proof pointer remain intact and receive no new read
certificate. View visibility updates are operational successors. Queued planning
carries the original dispatcher attempt and captures the live registration and
lock as exact transaction dependencies, while those operational rows never
become semantic support for generated content.

New definition regression source exercises real-backend bundle rollback,
disconnected organizer races, exact durable replay, changed request conflicts,
missing contributor restrictions, invented source ownership, tombstone refusal,
and a committed partition beside a failed partition. It remains unexecuted until
the storage, inventory, read-proof and queue consumers are integrated and centrally
validated. The original model/provider dispatch checks are preserved.

Full-graph reconciliation also guards discovery membership. Each partition
captures its epoch, rereads the complete original entity set, and rebuilds its
inventory before any provider call. A new or changed entity, or a different
curation fingerprint from the initial quota scan, refuses that partition and
requires re-preparation. Other security partitions participate only in this
membership check, not its semantic support. The unchanged-cache path verifies
complete membership and the epoch captured before discovery. Direct calls with
an explicit authorized candidate subset retain that separate subset contract.

The withdrawal receipt retains complete original Hidden definition after-images,
not just root IDs. Its visibility successor passes that saved image to the
storage owner's guarded hide operation. The transaction refuses a newer root,
including Hidden→Published→Hidden changes, rather than adopting the root observed
at delivery time. Repeated delivery of the same current image changes nothing.

A full-graph partition's receipt identity is stable for its security envelope and
original work item. Candidate membership remains in the semantic input
fingerprint. A retry with changed membership therefore conflicts with a committed
partition instead of inventing a second operation identity. Explicit direct
candidate subsets retain distinct request scopes.

Managed Wiki compilation now carries one storage-prepared publication authority
from the original invocation through every security partition and article. The
proof retains the original optional worker attempt and exact optional managed
registration; legitimate synchronous managed calls have a grant without a fake
worker claim. Planning, organization, writing and verification check the same
proof before each provider step, including retries. Graph context compilation,
article cache returns and the final article transaction use that same proof.
A later epoch or a successful definition commit cannot replace a cancelled
original claim or revoked registration. Explicit trusted direct-library calls
retain their separate contract.

The compiler tests use both actual storage backends and ordinary work creation
and claiming. They cover a successful definition-to-article-to-cache sequence,
cancellation before cached reuse, cancellation inside planning before the next
organization call, and cancellation inside verification before publication. The
last case requires the already committed definition receipt to survive while no
article is written. These are authored source tests, not execution receipts.

The managed withdrawal visibility successor also carries the same captured
publication authority. Its transaction checks both the receipt's exact Hidden
root after-image and the original claim/registration. Revocation between the
canonical withdrawal and the visibility update cannot be absorbed by a later
lookup. The trusted direct withdrawal path retains its exact-root guard.

Article preparation remains parallel within a security partition. The planner
captures one epoch after committing that partition's definitions, prepares each
context and provider result without publishing, and sends all successful fresh
articles to one storage batch transaction. That transaction guards the original
epoch and original publication authority, writes every revision/dependency/event,
and advances the epoch once. A sibling therefore cannot invalidate another
sibling merely by publishing first. Provider failures remain explicit per-article
results; a batch refusal publishes none of its prepared fresh outputs. It does
not rerun providers or replace their original context epoch.

Already published cache results are checked under the original preparation epoch
and revalidated as exact current projections after any sibling batch. Distinct
security partitions still execute separate complete preparations and commits in
sequence, with explicit aggregate partial progress. A new both-backend compiler
oracle blocks both real writer seams on a barrier, requires both preparations to
be present before any article write, and checks two successful publications with
exactly two writer and verifier calls. The original-proof accessor supplies exact
once-captured grant/lock rows to definition and withdrawal canonical read sets;
foreign-space compiler calls refuse before reading inputs.

Runtime11 fixture repairs preserve the actual producer and policy contracts. The
historical Watch budget oracle now completes its real work through the captured
lease-token transition, rather than editing Running state through generic update.
The ReResolve positive uses the actual named space's valid security policy for
its complete Source/Doc/Revision/Assertion/Entity chain; an empty policy string is
not a valid positive fixture. No production guard or expected evidence result was
weakened, and the repaired tests remain unexecuted in this worktree.


Reprocess source dispatch refusals identify the failed original-authority stage
(selection, saved intent, immutable revision, receipt or admission run) without
including record contents. The checks and refusal type remain unchanged. This
makes the native published plain-metadata retry failure distinguishable in the
next centrally scheduled runtime pass; it does not claim a repair or permit
missing producer history.

Public MCP reprocess creation first prepares a new job from the selected space's
actual configuration envelope. It authorizes that prepared job before asking the
storage owner to atomically compare the original configuration and create the
job, private producer and initial handoff. A classified space therefore needs
only its own policy clearance; default-policy clearance does not authorize a
classified job. Missing space configuration refuses creation before allocation.
Source selection and each selected contributor remain independently authorized
when the job executes. The trusted low-level repository creation APIs retain
their separate contract; this change does not relabel existing jobs.

The MCP regression source covers policy levels 3 and 7 with no default clearance,
exact job/work envelope inheritance, successful progress reads, revoked clearance
refusals without additional jobs/work, and missing-configuration refusal. The
storage owner separately tests configuration races and transaction rollback.
These source tests have not been executed in this worktree.

The retained pipeline `create_reprocess_job` factory also uses prepared creation
and the same atomic initial handoff. Its caller does not need to reconstruct a
second work envelope after job creation; `ensure_work` reuses the original handoff.
Only the explicit low-level repository creation APIs retain their trusted
caller-supplied-envelope contract. A factory regression checks classified scope,
original actor and idempotent initial handoff lookup through the real repository.

Private reprocess execution roots are now explicit. The physical dispatch budget
walk checks each job's original and current limit, then follows an `Inherited`
root's actual original Work/Run/fence through the storage-owned verifier. Only a
proved `Independent` admission terminates that execution ancestry. Source
operations apply the same rule even without a claimed admission attempt. The
existing shared 256-step cycle-aware Source/Work walk is preserved; selecting
content from an older source generation does not create an execution parent.

The real-backend budget oracle now creates an inherited job in an actual
canonical transaction under the original claimed parent and persisted run. It
requires inherited finite/current limits to survive the child's unlimited local
budget, allows a separately admitted independent root, preserves restrictions
after the real parent completes, and refuses a changed original run fence. The
new required private-origin field has no default-to-independent interpretation.
This source depends on B's private parent control and A's canonical admission
hooks; current standing-grant checks remain a separate required dispatch seam.

Retained Wiki inventory captures each approved event's original assertion rows
and immutable document evidence, as well as its source references. Read
eligibility alone does not substitute for those transaction dependencies. The
same original security partition applies to each contributor; appending a
classified citation cannot enlarge a public context's authority. Planner and
article preparation futures are boxed at orchestration boundaries so concurrent
preparation does not inline the complete nested state into each caller's stack.
The existing publication, revocation and unchanged-input regression assertions
are retained. Stack-overflow resolution awaits centrally scheduled execution;
no stack-size override is part of this change.

Immutable evidence may materialize a node's quoted text even when the persisted
node stores `text: None`. Inventory preparation rereads and validates the exact
stored node, compares every field other than that verified text adornment, and
retains the stored row as the canonical dependency. Subsequent assertion
validation therefore sees the same original row. The approved-event regression
checks this equality and rejects a real stored-node span change after capture.

Maintenance request fingerprints recursively sort JSON object keys while
preserving every field, scalar type and array position before the existing
SHA-256 digest. This makes captured nested metadata stable across native storage
map ordering. Existing stored fingerprints are never refreshed or relabeled:
an incompatible historical digest still refuses replay and requires a separately
prepared operation. Source-support fingerprints remain a distinct contract.

The retained-event Wiki fixture creates its Source with the original complete
subject classification inherited by its assertion and immutable evidence. It
does not promote an unknown Source classification during maintenance. The Wiki
replanning fixture persists its changed entity summary and uses the returned
stored row, including its timestamp, before requesting another plan. Both
fixtures retain the exact-current-row and original-source-authorization guards,
the raw-node change refusal, and the unchanged-article call-count assertions.

Reprocess dispatch allocates its source and retained-maintenance branch futures
separately. Retained maintenance also separates selection, resolution preparation,
recompute, budget refusal and canonical commit futures. This bounds their
composition inside the ordinary queue worker while preserving the original
attempt, selected stages, budget checks and single maintenance transaction.
A type-size regression checks the actual direct/attempt dispatch and retained
maintenance futures without constructing or polling them. The original dispatch
scenario remains the runtime oracle; no stack-size override or stage change is
part of this repair.

Source reprocess completion now honors a requested `ReMaintain` stage, including
an ALL-stage job whose immutable source selection is empty. After every admitted
source operation has a real current completion certificate, the coordinator
prepares global stale-slot invalidation, deterministic recomputation, its durable
maintenance seed, source certificate artifacts, job transition and receipt in one
canonical plan. The seed binds the original selection/configuration/operation
request digest. It records invalidation once; continuations recompute only the
remaining stale slots. Current published support and the original work attempt
remain storage-owner transaction preconditions.

Deferred conflict groups or skipped/still-stale slots keep the job `Paused`, with
no completion timestamp, and return actual pending groups to the existing queue
handoff. Each continuation uses its captured epoch in the receipt scope. Already
committed source certificate artifacts retain their original run and bytes;
changed certificate facts or seed payloads refuse. Jobs without `ReMaintain` keep
their selected-stage behavior. This is retained maintenance of published support,
not another source-finalization proof or whole-source publication transition.

`source_completion_tests.rs` supplies real embedded/native controls for the
original ALL-stage empty-source path, inline single-value recomputation, real
pending divergent groups, unchanged finished slots/seed on continuation, omission
of unrequested maintenance, changed-payload refusal, an original-before-image
collision that must leave job/seed/claim/slot/outbox/receipt unchanged, and stale configuration or
original-attempt refusal. A separate artifact-retention control does not mint any
source operation or publication proof. These controls are source authored only
until the central validation lane executes them; the original worker scenario
remains the queue/blocking oracle. Source coordinator child futures are boxed
without changing lease, stage, provider, or stack-size policy.

The completed-job shortcut also requires its original requested-maintenance seed
and exact request digest. Missing or changed historical evidence refuses; retries
do not reconstruct the seed or silently run maintenance under a completed status.
The two-backend withdrawal control first completes a real ALL-stage job, then
changes and removes that actual seed and checks refusal without job/proof repair.

The duplicate-create completion control exercises the canonical preflight guard:
all before-images are checked before effects are written. It does **not** prove
late rollback. Separate existing storage controls
`embedded_atomic_bundle_faults_replay_and_space_isolation` and
`surreal_atomic_bundle_faults_replay_and_space_isolation` use the actual
`CanonicalFault::AfterFirstEffect`, `BeforeOutbox`, and `BeforeReceipt` hooks in
`crates/storage/src/canonical_commit_tests.rs`. Their generic transaction coverage
is distinct from the completion-path preflight control; no new completion-specific
late-fault execution is claimed.

Completion request encoding sorts a copy of the original operation-ID multiset.
Initial Source selection order and restored WorkArtifact order can differ; that
traversal difference does not change the seed identity. Duplicate IDs remain in
the fingerprint, and membership/configuration changes remain distinct. The pure
encoding control covers all six permutations, replacement/additional/substituted
duplicate/missing IDs, an unchanged caller vector, and configuration rotation.

The single-value completion control keeps the real minimum evidence threshold
unchanged, including its clamp to at least one. Its approval positive creates an
ordinary source-less Doc, immutable active DocRevision, resolved Assertion and
exact EvidenceRef before job preparation. The shared read-eligibility reader
must recover the original quote, revision and document with no Source. The paired
zero-evidence control requires `Disputed` with the actual threshold rationale.
Both variants also exercise omitted maintenance, which leaves the original
candidate, slot and absent seed unchanged. No Source publication certificate,
head, or support ledger entry is invented and no predicate bypass is configured.

The graph-change Wiki control identifies both originally planned article roots.
Updating Zebra's persisted summary must replan the graph and regenerate Zebra's
article while returning Eve's exact original immutable revision unchanged. It
asserts both memberships, stable view IDs, one changed article, changed Zebra
revision/dependency hash, and unchanged Eve revision before counting exactly one
additional writer/verifier pair. This qualifies article reuse without assuming
that the second planned candidate has no article of its own.

Definition planning now boxes the actual inner authority capture, plan capture,
receipt replay, audit and commit futures, including grant/publication checks
inside the capture helpers. The outer 88-byte wrapper observed in Runtime19 did
not establish the size of the inner future it constructed. A no-construction
type control covers that inner orchestration, authority, plan and inventory state
against the ordinary stack budget. Bounded phase/size debug events mark the real
orchestration boundaries without recording graph IDs or content. This does not
change authority, original epochs, provider calls, plan effects or publication
order; the original four source-backed Definition scenarios remain the runtime
oracle, and the frame repair is unproven until the central lane runs them.

The Runtime21 original Definition fixtures exposed a deeper commit-stack limit
on both backends and in both scenario binaries. Their trace reached the real
maintenance commit after authority capture, inventory, planning and auditing.
A 456-byte commit future did not establish which descendant overflowed:
request preparation and the eventual canonical transaction were both inside it.
Read-only inspection of that already compiled executable found a 109,736-byte
poll frame for decision evidence validation and an 89,432-byte assertion evidence
poll frame. These are machine stack reservations, not future object sizes.

Maintenance now boxes the existing assertion, relationship, attribute and event
validation children separately, including the shared reader's original completed
render proof. Preparation, support capture, request assembly and canonical apply
emit only static debug phase names and numeric future sizes. A completed phase
is emitted only after the actual canonical commit returns successfully. Every
original input, immutable evidence check, source support, authorization envelope,
read-set observation, epoch, attempt, audit, outbox and receipt remains on the same
path. This change neither certifies a source nor creates a second transaction.

The new type-only control bounds orchestration futures at 8 KiB and individual
evidence futures at 32 KiB without constructing storage or polling a future.
It prevents reintroducing large nested state machines; it cannot prove a bound
on cumulative machine stack use. The four original Definition runtime fixtures
remain the acceptance controls, including actual receipt, split-source policy,
Graph context and pending-generation assertions. This source checkpoint has not
been compiled or executed locally and makes no claim that their overflow is
resolved before central validation.

Runtime22's first two original pending-generation Definition scenarios reached
`maintenance.commit.done` before overflowing later, during article preparation.
The actual commit success does not establish a published article or a passing
Definition oracle. Read-only inspection of that executable found a 149,952-byte
article-preparation poll frame and a 124,472-byte definition orchestration poll
frame, excluding saved registers. The interval before article publication still
contained multiple children, so these measurements do not identify the precise
failing instruction or constitute a dynamic stack trace.

The article stream now has its own boxed phase with the same sibling epoch,
concurrency bound and ordering. Article preparation boxes its existing authority,
epoch, definition, context, evidence, cache and current-projection checks. The
source-evidence augmentation and catalog also box their original evidence,
projection, published-source and reader-current checks separately; all original
scope joins, immutable spans, source IDs and dependency timestamps are retained.
The writer and verifier retain every original guard and retry; static phase names
identify their boundaries without logging graph IDs, prompts or model output.
Synchronous view/revision assembly occupies a separate non-inlined function, so
its temporaries are absent while preparation polls storage or provider children.
The same current-projection and epoch checks follow that assembly, and the same
batch publication waits for every sibling preparation. The direct article compile
entry point uses the same bounded preparation and publication children.

A type-only regression covers the real article join, private preparation, direct
compilation, source-evidence augmentation and catalog futures at 32 KiB. It
constructs no storage and polls no future. This constraint is distinct from
machine stack qualification. The four
original Definition scenarios, including their actual receipt, article,
Graph-context, split-policy and pending-generation assertions, remain required
central runtime controls; no larger stack or relaxed timeout is introduced.

Runtime24 compiled the original source and all 486 selected tests. Its four
original Definition cases advanced through article epoch and definition reads,
then overflowed after the `article.context` boundary. The scoped trace does not
identify an instruction inside that Graph context. Static inspection of the
actual executable still found a 94,760-byte article-preparation poll frame,
although its stored future was only 7,528 bytes. That complete editorial frame
remained active while polling Graph context and its descendants.

Article preparation now joins two private phases. The first retains the original
space/authority/epoch and definition guards, then prepares and augments the exact
Graph context and evidence catalog. Its owned result moves in a box to the second
phase, which retains the original dependency identity, cache checks, writer and
verifier retries, assembly, current-projection check and final epoch check.
The second future is constructed only after the first has returned. Consequently
its cache/writer/verification poll frame is absent during Graph preparation.
Both extracted operation bodies are unchanged, including statement order, guard
arguments, error text, source/security joins and prompt construction. No source
publication or provider authorization is inferred from the phase transition.

A separate type-only control bounds the joining future to 1 KiB and both actual
phase futures to 32 KiB, without storage or polling. This bounds stored state;
it is not a substitute for measuring compiled frames or for the original four
Definition scenarios. Source completeness, formatting and static extraction
identity are the local evidence for this change. Central validation still must
establish the actual final articles on the original stacks and deadlines.

The same Runtime24 executable reserves 438,648 bytes in the `KnowledgeViewRevision`
current-read poll frame. That shared descendant validates Graph and entity-view
projections; this static measurement does not identify the faulting instruction.
Its revision traversal now boxes the existing evidence, repository, projection
and content-support children. Each typed dependency's original timestamp check
and domain-subject projection/full-record equality check runs in a private typed
child, instead of duplicating those temporaries for every record type inside the
whole traversal. The macro still loads the same repository row first and refuses
a missing row or storage error in the same order.

The traversal retains its exact scope/deletion/authorization/support check before
visited deduplication, citation order and immutable spans, 2,048-dependency budget
including descriptors, table/type parsing, published-source timestamp rule,
DocNode and SourceWatchCheck projection rules, and nested-view ownership/timestamp
and path-cycle checks. The private typed helper uses each record's existing
`CurrentRead::read_base()` for the same `base.updated_at` observation. It introduces
no alternative source authority or filtering policy. The original weaker
projection-only branches are not replaced by full-record equality.

New source controls exercise real embedded/native stored entities through the
typed child: a stale observation must refuse before authorization, a matching or
absent optional observation retains authorization, and a deleted row remains
ineligible. A separate unpolled type control covers actual Entity, Rel and
KnowledgeViewDefinition child futures. Existing read-policy, source-support,
Definition and full erasure oracles remain unchanged. These tests are authored;
no local Cargo or backend execution occurred for this checkpoint.
