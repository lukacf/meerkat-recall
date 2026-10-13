# Preview work recovery contracts

Queue ownership is represented by the durable `work_attempt` row, keyed by work
item identity. Every claim atomically advances its never-reset generation and
issues a new lease token. `work_item.attempts` is the execution retry counter;
configuration blockage may refund this counter, and authorized retries may reset
it. Neither action resets attempt authority. Terminal archival retains the
attempt row. The `_attempt_fence` payload member is overwritten by the queue at
claim and carried from that claim into publication, never reconstructed by reading
a newer attempt.

The queue scans expired Running work across every space independently of runnable
candidates. Missing lease expiry also makes abandoned work recoverable. Completion,
renewal, release and failure require the captured token and an unexpired lease.
There is no force-completion fallback. Canonical publication uses
`check_attempt_fence` or `ATTEMPT_FENCE_SQL` inside the effects transaction. Queue
completion alone does not complete a source operation or establish searchable
coverage; the source finalization manifest owns that condition.

Every runner attempt extends its lease past its execution deadline. Execution
timeouts use the same bounded policy as transient errors. Configuration-blocked
work remains queued with a bounded retry interval and an explicit reason, while
stale immutable input reports a repair requirement instead of success. Typed
security/input failures cannot become retryable merely because their message
contains a word such as `timeout`.

Knowledge-view invalidation and truth-maintenance delivery start from durable,
space-local `outbox_checkpoint` rows. New consumers start at sequence zero. Effects
are idempotent and delivery is at least once: acknowledgment occurs only after the
complete batch applies, and a checkpoint compare-and-swap failure replays effects.
Malformed event records, commit failures and enqueue failures do not advance a
checkpoint. Deterministic successor identities prevent duplicate enqueue on replay.
Snapshots must include events and checkpoints together; restoring an older data
snapshot must never retain a newer consumer cursor independently.

`WorkRunMetrics.custom.usage` distinguishes `not_started`, `unknown`, `estimated`
and `measured`. Reservations (`reserved_tokens`, `reserved_budget_units`) are
separate. Planned tokens never populate `tokens_used`. Received provider usage is
measured; missing receipts after dispatch remain unknown even after failure or
timeout. The historical EUR-named budget fields are retained for storage shape,
but the current token multiplier is an abstract budget policy, not provider
pricing or a currency claim. Usage receipts explicitly identify the budget unit;
the runner does not manufacture `estimated_cost_euros`. Reservation reconciliation
updates the attempt ledger and shared daily guard atomically and once, including
zero-usage failures.

Source regression coverage is authored for quiet named spaces on both backends,
claim turnover, expired-owner refusal, archive/retry generation preservation,
configuration-blocked retry allowance, checkpoint restart/CAS isolation, typed
timeout policy and reservation-versus-measured usage. Compilation and execution
remain pending the coordinated source-complete validation checkpoint. Snapshot,
erasure replay and initialization mechanics are implemented by the durability
package and have separate qualification evidence.


Accepted source handoff uses `source_queue::enqueue_accepted_source`. The original
`_publication` travels through successors, while `_attempt_fence` never travels
from a predecessor. Deterministic IDs include named space, kind, idempotency key
and accepted generation/revision; an exact replay must match the durable payload
and security envelope. Changed semantics under one identity return an error.
Truth-maintenance outbox acknowledgment transfers the complete ordered event ID
batch to deterministic durable work. Missing events fail delivery; mutations are
performed under the later claimed attempt. Retained queue/history queries check
all database statement errors.

The runner resolves explicitly configured named spaces and current operation
bindings at dispatch; embedding and maintenance discovery exhaust configured
space pages. No empty/error space discovery silently introduces a default space.
Optional deterministic truth maintenance remains usable without an LLM binding.
Source-queue replay and fence inheritance regressions are authored for embedded
and SurrealDB memory backends. They have not been executed yet.


Provider accounting persists `provider_call_receipt` before each physical
request, including transport retries. Receipts retain safe binding/config IDs,
space and attempt/activity identity; they contain no prompt or credential.
Cumulative usage snapshots replace per-call totals idempotently. A request
without a complete measured receipt remains unknown, including after timeout.
Known partial totals remain visible beside that uncertainty. Late usage can
update the original receipt but cannot publish under an expired attempt.
Worker completion aggregates these receipts, never a planning estimate. Idle
embedding batches install the same sink with a distinct durable activity scope.
The receipt regression source covers both storage backends, duplicate/older
snapshots, physical retries, late receipts and wrong-space rejection.

Reindex dispatch processes one bounded page and uses a pending-dependency retry
without exhausting failure allowance while coverage remains. Activation remains
an explicit administrator operation. Whole-source missing coverage uses the same
retry class. No Cargo validation has been executed for this checkpoint.


The runtime now checks the original accepted source revision before stage writes,
materializes a distinct physical document per acceptance, and carries exact
metadata bundle IDs through the chain. A sealed manifest precedes extraction and
embedding fanout. Source node commits register staged bundles only; the finalizer
publishes the complete revision. Declared embedding jobs validate exact immutable
UTF-8 spans and fail on missing nodes, truncated text or partial returned coverage.
Successful vectors alone cannot substitute for the declared job's completion.
All-space recovery pages accepted operations, replays recoverable attachment
staging, restores deterministic manifest children and finalizers, and schedules
pending explicit reindexes. An error in one operation does not starve other
spaces; the sweep reports incomplete recovery after visiting them.

Predecessor queue completion and successful originating runs gate successor
processing. A resumed planner adopts its exact sealed plan under the new original
attempt, preserving selections and IDs. Stale input schedules repair from the same
immutable accepted source within bounded retry allowance; superseded/withdrawn
inputs cannot be revived. Source lifecycle projections change in finalization.
The renderer/tree replay implementation and full-coverage planner are owned by
Grounding; source acceptance/outcomes by Integrity; source staging by Intake;
registration/embedding completion proofs by Finalization.

Whole worker attempts, embeddings, maintenance, retention and outbox deliveries
hold storage activity scopes. Dedicated wiki threads propagate dispatch scope and
cancel their future when the awaiting worker is cancelled, so timeout cannot
leave that producer running. Exclusive erasure drains these activities before
physical purge. Startup/schema/export/restore mechanisms are the joined Durability
implementation; no implicit serve migration was added.

Unknown provider consumption retains its reservation, distinct from measured
spending. Durable settlement replays completed runs across hot and archived history
when a full usage receipt arrives, and its ledger transaction reconciles once.
Queue completion resolves accounting from its original attempt receipt, never
from the latest mutable source decision. Daily-cap lookup errors block dispatch.

Additional regression source covers both-backend replacement identity/recovery,
pending predecessors, cancellation, strict embedding spans, pending-dependency
classification through decorated storage errors, and late-receipt settlement.
Formatting and whitespace checks only have run. These joins are source checkpoints,
not a compilation or runtime qualification claim.


Background truth-maintenance and resolution dispatch now binds the original
attempt into atomic maintenance helpers. Stale decisions schedule fresh
recomputation and cancel the replaced decision explicitly; they do not report
obsolete inputs as successful commits. Bounded recompute batches durably enqueue
continuations when slots remain. Source operation Processing and safe typed failure
codes are persisted through Integrity's source-and-attempt transaction. An already
completed exact operation replays without rerunning stage side effects. Two-backend
regression source covers configuration blockage/resume and stale attempt refusal.
Retained ingest priority is read from the immutable accepted `_intake_priority`.

Broader legacy maintenance sweeps and source reprocessing still require the
separately coordinated lifecycle/canonical owner joins. The B runner's activity
scope covers their erasure drain and usage accounting; that scope does not itself
make their canonical mutations atomic or attempt-fenced.


Both outbox consumers use `outbox_delivery::deliver_batch`: one storage activity
covers cursor load, ordered event fetch, all consumer effects, and acknowledgment.
This closes the restore window between knowledge-view invalidation and its cursor.
Errors after partial effects retain the prior cursor; deterministic queue insertion
makes retries idempotent. Regression source drives the actual shared delivery
function through a failed effect, repeated delivery, and a checkpoint CAS race.
Independent qualification can inject its restore boundary through this same
production callback seam. No product runtime tests have been executed locally.

Retained dispatcher unit fixtures now persist real queue claims and original
attempt fences, dispatch with explicit injected domain services, save completed
producer receipts, and acknowledge through the token-scoped queue. Completed roots
remain in test history. Provider-construction denial and deterministic provider
transport fixtures remain separately scoped; unit tests do not select ambient
credentials or perform paid calls.

TM dispatcher regression fixtures now retain physical document/revision/node and
assertion rows with matching quote hashes. Deterministic detection asserts its
committed slot and merged evidence immediately; it does not expect a deferred
commit task after the maintenance owner's atomic-detection join. This is test
source alignment, not a runtime pass claim.

General work-item updates and DAG dependency transitions cannot enter or replace
Running state. Callers claim through the queue/repository and retain that exact
claim for `DagExecutor::transition_attempt` or the token-scoped queue. This closes
the retained generic transition bypass while preserving promotion, retry, release,
cancellation, and completion through their appropriate authority. Both backend
regression sources assert generic mutation rejection, unchanged original fence,
and successful completion by its real token.

Restart usage recovery also closes unfinished runs whose captured attempt is no
longer live. The run outcome and original lease inspection share a transaction;
a successor's active lease cannot keep its predecessor run falsely open. Recovery
never adopts the successor fence. Interrupted outcomes remain failed, provider
receipts with unknown totals retain reservations, and late complete usage settles
once. Both backend regression sources cover live-attempt preservation, lease
turnover, interruption receipt, unknown reservation, and late settlement.


An accepted-source bootstrap only enqueues a configuration-pinned triage chain.
Every semantic successor retains the original `_publication`,
`_source_configuration`, and derived `_source_plan`; attempt authority is still
freshly claimed for each stage. A legitimate configuration change creates distinct
work, metadata, validated bundles, embeddings and finalizer identities for the same
immutable accepted revision. The old chain cannot recapture current configuration
or adopt a later completed receipt. Recovery discovers this condition in idle named
spaces, without depending on an HTTP request or a Runnable predecessor.

A replacement planner compares the old manifest digest, original live attempt,
current configuration and absent completion through the finalization repository's
atomic replacement API before fan-out. The previous sealed set becomes immutable
source-scoped history. Rendering can reuse the immutable accepted document recipe;
changed input or rendering recipes require a new accepted revision. The current
manifest remains the publication authority. Configuration-versioned work cannot
publish an old candidate under a new configuration.

New regression source covers exact context inheritance, distinct queue/artifact
identities, embedding queue identity parity, malformed plan rejection, bootstrap
behavior and both-backend quiet-space recovery with an unchanged source head.
These tests are authored, not executed locally; finalization and grounding own the
joined atomic manifest and producer-artifact checks. No local Cargo was run.

Metadata staging also retains the original publication/configuration/plan tags and
uses a distinct ID per plan and producing WorkRun. Exact same-run replay compares
the stored data and envelope; a later producer cannot overwrite it. The accepted
input and captured configuration are checked again before storing output. Final
publication still depends on the manifest owner's atomic proof, not these read
checks. Regression source distinguishes metadata identities across plans and runs.

Reprocess dispatch passes its original claimed AttemptFence into the maintenance
owner's execution adapter. Pending accepted operations or unfinished maintenance
batches keep the work recoverable with a typed dependency wait; they cannot mark a
job successful early. Resolution continuations inherit the input envelope and get
fresh queue claims, with no dependency on the waiting parent's Done state. A queue
regression authors the parent-wait/child-runnable handoff and exact replay. Source
reprocessing no longer constructs an unused truth-maintenance model service first.

Embedding reindex dispatch now supplies its original accepted reindex ID and
claimed AttemptFence to the fenced reindex adapter. Its owner checks this authority
inside vector and progress transactions. A persisted page remains a dependency
wait until coverage is complete; the worker does not activate the staged index.
The owner's both-backend regression source covers lease turnover and stale writes.
Visibility/control changes also require the coordinated index-generation hook;
the runner call alone does not establish that storage invariant.

Failure bookkeeping leaves completed, superseded and terminal source receipts
unchanged while allowing an obsolete queue item to finish under its own original
lease. A terminal source receipt fails stage admission explicitly. These checks
avoid a loop where a rejected source-progress update strands the item Running,
then each expired-lease recovery repeats the same rejected update. Both-backend
regression source verifies the old queue acknowledgment and unchanged new receipt.

Accepted-source triage uses the original WorkRun as its sole reservation owner.
Its decision stores planning estimates without creating a second future-work
ledger or an unfenced legacy upgrade task. Direct retained triage APIs keep their
existing reservation contract. Budget-denied accepted modes/upgrades now return a
typed recoverable wait, retaining the intended mode and original source plan;
they do not acknowledge a stage with no successor or certify downgraded coverage.
Both-backend accounting regression source checks the unchanged original ledger,
no duplicate reservation, and no unversioned upgrade enqueue.

Source embedding captures a sealed manifest/lease write authority before obtaining
its model service and keeps that same authority through every vector batch. The
embedding repository checks it inside the node/vector transaction, including empty
batches. The governed provider's captured configuration must match the original
source plan before dispatch. Paragraph/chunk inputs use immutable revision spans;
only typed Summary nodes use captured generated text, with missing summaries
rejected before inference. A recording-provider regression source checks those
actual dispatched inputs. F/L own transactional stale-claim/node-mutation oracles.

Promotion model options are constructed only for Graph finalization. Metadata and
Retrieval completion do not depend on an unrelated optional promotion binding.
Retrieval/Graph publication still requires the manifest owner's matching active
DocumentEmbedding and QueryEmbedding bindings and complete vector coverage.

Before inference, source embedding also compares each owned node/revision input
against the sealed authority snapshot. The later transaction check remains. A
transient edit that is restored before storage cannot cause a vector for different
bytes to pass the final check; the provider consumes only the validated copies.

Trusted standalone maintenance can allocate real proposal provenance through
`work_run_repository::create_trusted_proposal_context`. One transaction creates the
linked terminal WorkItem/WorkRun for context allocation only. Its `provenance_only`
metric, absent lease/attempt fence and no-model usage distinguish it from a sweep
or publication receipt. The caller supplies the prepared proposal union envelope
and sources; only a scope digest is retained. Exact replay preserves original
records, and changed, partial or deleted contexts fail without reconstruction.
Queued maintenance continues to use its actual current WorkRun instead. Two backend
regression sources verify real links, scope isolation, replay and deletion refusal.

Reprocess jobs are durable queue intents. `ReprocessJobRepository::create_with_work`
atomically creates a new Pending job and its initial `execute_reprocess_job` item.
Canonical predicate approval instead includes the Pending job in its complete
canonical mutation. The worker pages Pending/Running jobs across all spaces,
including unconfigured quiet spaces, and calls `ensure_work` against the exact
observed job snapshot. Queue identity derives from the original job ID and space;
security, subjects and provenance follow that job. A crash after canonical commit
and before eager enqueue is recovered without a client request. Existing hot or
archived queue outcomes, retries and leases remain unchanged; this repair does
not authorize retries of Failed/Cancelled/Done work or resume Paused jobs.
Both-backend regression source covers atomic rollback when the queue ID conflicts,
original-lease preservation, terminal replay, changed-envelope refusal, and
paginated recovery of a committed intent with no configuration or queue entry.
These new regression sources have not been executed in the owner worktree.
Native reprocess/context replay compares explicit record IDs separately from
record content serializers, which omit IDs, and uses the shared persisted-native
value normalization for nested `NONE` fields. The original field comparisons and
authorization envelope checks remain mandatory.

`provider_usage::source_operation_usage` projects up to 256 original operation
IDs from owner receipts, paged hot/archive WorkItems and WorkRuns, and physical
provider receipts. It validates the original publication, configuration and
attempt lineage, counts each physical dispatch once, and includes failed plans
and late usage. Missing usage or execution evidence remains unknown. Input and
output totals are independently unknown if a contributing receipt is incomplete.
The projection's `complete` requires the owner's Completed state and closed,
known execution evidence; it never advances an operation. Measured tokens have
no fabricated EUR conversion: a dispatched call leaves `monetary_euros=None`.
Known zero requires actual closed no-dispatch evidence for a completed operation.
This read-only projection is not a dispatch budget admission guard.
Both-backend regression source uses actual accepted operations, queue claims,
WorkRuns and durable observer receipts, including timeouts, archival, duplicate
usage observations and late receipts; no provider network request is made.
Usage reconciliation also refuses an empty physical receipt set when prior
metrics report Unknown, Measured or Estimated consumption, or contradictory
nonzero consumed tokens. Lost evidence cannot reset consumption to NotStarted
and release a reservation. Both-backend accounting regressions cover this rule.
Fresh-key A→B→A acceptance reuses immutable source input while materializing a
new physical document from the full returned publication fence. Regression
source now exercises all three actual acceptances, distinct operation/generation,
distinct WorkItems and physical Docs, unchanged first immutable revision/input,
rejection of the original worker, and quiet-space recovery preserving the third
operation. Fixture replacements capture the real authorized source pre-state.
The production materializer already derives this identity from the original
WorkItem fence; it never reuses the first StoredRevision operation identity.
The native knowledge-view invalidation consumer treats a missing mandatory
`dependency.view` as data-integrity failure. It cannot filter malformed pointers
out of a successful batch and advance the durable checkpoint. Regression source
runs the actual delivery callback, verifies that malformed stored input keeps the
old checkpoint, then repairs that input and replays the same event before ack.
The periodic maintenance loop explicitly calls the owner's atomic sweep wrapper
with `attempt=None` and `work_run=None`, under its existing full activity scope
and durable background provider observer. It is a standalone activity, not a
fabricated queue claim. The maintenance owner allocates a real provenance-only
context only when actual Draft proposals require it and observes that context in
the canonical read set. Queued reprocessing still receives its actual original
attempt and WorkRun through the separate attempt-aware execution wrapper.
Queue settlement gives the original attempt WorkRun's ledger priority over older
payload/decision references. It requires a closed run, captured attempt and durable
physical receipt summary; missing run/usage evidence retains the reservation and
is not settled as zero. A real closed NotStarted run can release an unused
reservation, so ordinary failures before dispatch do not consume model budget.
Retained tests distinguish this proven no-dispatch case from missing accounting
history and verify that a conflicting older payload ledger cannot take ownership.

WorkArtifact full-record updates replace nested data on both backends. Native
`CONTENT` updates now match embedded replacement, including conditional status
updates, so removing coverage or validation fields actually removes them. Status,
space and live-row guards remain intact. Both-backend regression source rereads
stored data and checks nested deletion, stable identity/envelope and stale-status
refusal. This repairs the focused publication test's actual persisted precondition.

### Physical usage completion

A physical provider receipt carries a serde-default-false usage_complete marker.
Cumulative token snapshots remain useful partial measurements, but they cannot
settle a reservation or become complete original-operation totals until a real
terminal response and both token fields have been observed. The active SDK stream
marks completion only on successful Done, before yielding that terminal event;
stream errors, missing Done and dropped streams leave usage unknown. The embedding
transport marks completion only after successfully decoding the response body.
Neither hook grants queue or publication authority. There is no new pricing claim:
physical calls still have unknown monetary cost without a rate card.

The same original call can receive a late terminal marker after its work attempt
has timed out or been archived. Receipt updates are monotonic and idempotent;
recovery settles the original reservation once. Completion without usage remains
unknown. Old receipt JSON has no terminal evidence and defaults to unknown rather
than fabricating completion. Missing or physically erased receipts cannot be
recreated by a late usage/completion callback.

Regression source covers both storage backends for partial usage, late/duplicate
completion, reservation retention through restart/lease loss, archived source
operation totals, physical receipt deletion and legacy JSON. The actual model
HTTP authorizer/event observer is checked for drop, error, success and absent usage;
a loopback embedding response fixture covers successful decoding versus truncated
JSON. These tests are authored for the central validation lane, not run locally.

### Failures before execution

The runner's daily-cap lookup and cost-reservation failures now use the same typed
retry policy as execution failures. Actual cap/configuration refusals retain
bounded prerequisite polling and refund the unused execution attempt. Storage
outages consume the configured finite retry budget; integrity errors fail the
original claim. Every transition uses its original lease token, so a delayed
preflight result cannot change a successor. No WorkRun or provider usage is
fabricated for a failure before execution. Initial typed provider-configuration
resolution also preserves storage errors instead of relabeling them approval
refusals. Both-backend regression source exercises retry exhaustion, real budget
blockage, integrity failure and delayed-owner fencing; it awaits central validation.

The reprocess handoff repository exposes verify_reprocess_handoff for an actual
job/work pair already read by a trusted caller. It checks the existing complete
payload, kind, stable idempotency identity and authorization/provenance envelope,
including retained handoffs with historical random IDs. Paused/failed job status
does not erase that relationship. This is structural evidence for M's finite-job
guard, not an authorization grant, price quote, or replacement attempt fence.
The runner's initial resolve_space now also preserves the typed storage cause.

The private erasure reservation's typed StorageError::ErasureInProgress is a
transient scheduling interruption under that same finite retry budget. It is not
terminal and does not grant indefinite configuration-attempt refunds. Classifier
and both-backend original-claim transition regression source cover this exact
error, including nested PipelineError and stale-owner fencing. The transition
fixtures inject the typed refusal; independent erasure tests own the actual
reservation admission/release oracle. No error-text heuristic is used.

### Finite reprocess job dispatch gate

The physical dispatch observer loads its actual current WorkItem and compares it
with the originally captured AttemptFence before calling M's
reprocess::guard_physical_dispatch. The guard resolves existing durable job/intent
relationships; a payload label or caller role alone does not authorize billing.
This runs before writing a physical call receipt and before either embedding or
SDK HTTP authorization can attach credentials. A denied finite EUR request remains
not-started, and its explicit prerequisite refusal survives SDK error wrapping.
A late callback cannot adopt the successor's claim. Usage callbacks for already
sent requests still update only their original accounting receipt.

This join depends on M's exact guard implementation and original-operation lookup;
private cross-policy TM child lineage remains a coordinated authority question.
No pricing or universal hard currency cap is invented. Both-backend regression
source creates a real finite job/handoff/claim/WorkRun, verifies the installed
observer refuses before receipt creation, then explicitly removes the job's limit
and verifies ordinary admission resumes. Runtime validation remains central.

### Public external-worker lease contract

claim_work_item returns the actual committed opaque lease_token. Its caller must
retain that token and supply it alongside id to complete_work_item. Missing or
empty tokens are invalid parameters; stale tokens or changed authorized snapshots
return conflict_generation with retryable=false. The adapter never adopts a token
from the latest stored row or treats the worker_id label as lease authority.
Existing success/error options and tool names remain. External consumers must echo
the returned lease_token for completion.

Both operations require read:work_item and write:work_item on the actual record.
The storage claim/completion transaction also compares the authorized observed
security, subjects, completeness, lifecycle, update time and semantic work identity,
closing reclassification races after authorization. The original lease still has
to be live. Normal queue workers retain their existing captured-attempt methods;
the public completion path preserves success-aware durable usage reconciliation.

Both-backend regression source exercises the actual MCP claim/complete handlers,
classified row denial, missing read scope/token, empty token, worker-label refusal,
old token after reclaim and successful current-token completion. Storage regression
source injects concurrent envelope changes and verifies atomic refusal without
changing the queue or its attempt. C's typed MCP pipeline-error conversion keeps
private erasure reservations retryable. These tests await central execution.

Claim/complete coarse scope refusals use the same typed forbidden_space result as
actual-row refusals; an unavailable selected-space work row uses conflict_generation.
Claim rejects zero/overflowing lease durations and uses checked timestamp addition,
including durations that fit chrono::Duration but exceed the timestamp range.

### Dispatcher stack storage

Large work-kind phases are boxed at the actual await boundary inside the shared
dispatcher, including source finalization, truth maintenance, resolution patches,
and document/vector processing. Boxing only the caller cannot prevent a large
composed branch future from being constructed on that caller's stack. The worker
semaphore controls concurrency, not the size of an individual future.

`dispatcher_future_storage_stays_bounded_on_default_stack` inspects the actual
dispatcher future type without constructing or polling it and limits its inline
storage to 64 KiB. Existing work-kind execution tests remain the behavioral oracle
and must run with the default stack. The source checkpoint has not executed these
tests locally; central validation must establish both the size and runtime result.

### Physical authorization failure categories

The current-space resolver preserves its typed failure category when crossing a
provider SDK's string-only error callback. Missing/revoked approval remains a
configuration prerequisite; connection, transaction and erasure-barrier failures
use bounded transient retries; malformed stored evidence and cross-space authority
remain terminal. Generic `provider_authorization` text alone grants no retry.
These classifications do not settle usage: a previous physical dispatch still
requires its original terminal receipt or remains unknown. Regression source
checks the real SDK revocation callback, both storage backends' embedding resolver,
and wrapped typed failure categories.

### Native operational record round trips

Ingest decisions and deadletters use the shared native base-field conversion:
optional deletion is `NONE`, timestamps are datetimes, and source/work/run links
are native record IDs. Existing create/get/update/latest tests and attempt-owned
triage tests exercise these active repository paths. The retention snapshot test
now exports and restores through `StorageClient` with its original independent
journal, verifies removal of a post-snapshot work item, then checks archived native
fields across backend transfer. The physical usage receipt deletion oracle binds
its UUID record ID as a native value. These are source fixes for the central
runtime failures; no local Cargo or test execution was performed.

### Deadletter original-attempt ownership

`DeadletterHandler::deadletter_claim` takes the original claimed work envelope,
finishes that exact token through `WorkQueue`, and reconciles durable usage before
recording the deadletter. The retained ID-only `deadletter` operation records an
already acknowledged Failed outcome; it cannot acquire authority over Running
work. Its per-outcome deterministic ID allows replay if letter persistence fails
after acknowledgement. The two steps are recoverable, not one atomic transaction.
The letter inherits the work's authorization envelope. Administrative retry uses
the same failed-state compare-and-swap as the public retry route, preserving
attempt/error history and the monotonic lease generation. Regression source uses
real claims and checks that an old worker cannot deadletter its successor.

### Immutable private producer origins (migration 041)

Every newly created dispatchable WorkItem has an explicit private
`work_producer_origin` record written in the same transaction. Ordinary work and
managed wiki reconciliation capture Direct, meaning no queue parent; source work
still requires the original source-operation producer held by the publication
owner. A reserved reprocess tag cannot establish Direct authority.

A new ReprocessJob captures its original queue intent and finite-currency flag
before handoff materialization, including canonical job creation. Recovery checks
that original control and the actual current job in the handoff transaction; it
never reconstructs missing history from the current budget. `origin_for_job`
allows admission to inspect the original restriction before a queue row exists.
Finite-to-None retains the original restriction; None-to-finite is subject to the
current finite policy. There is no invented price or currency conversion.

TM child creation stores the original parent work/run/AttemptFence privately and
uses an opaque digest as the public idempotency key. Group children inherit the
actual ConflictGroup's complete authorization envelope, allowing valid work across
different parent/group policies without treating ancestry as an authorization
grant. Parent IDs, job IDs, source-operation payloads and public predecessor IDs
are not added to the child. Sequential children consult the private original run
for predecessor readiness; coordinator children can execute while their parent
waits for them.

The enqueue transaction verifies the live original attempt, actual run, group,
all private ancestry records and current original job. Replays preserve the
existing child and refuse a missing control. Archival retains producer records;
ordinary erasure may delete them with their referenced source/work/run/job rows.
They are reference-bearing private payload, not indefinitely retained controls.
Missing or changed evidence is Unknown/refused. Snapshot export/restore carries
the same records through the original-journal replay barrier.

The storage regression source exercises both backends, original budget changes
before queue materialization, actual claims/runs, group authority, concealed IDs,
child/grandchild replay, changed inputs, archived parents and deleted controls.
The embedded branch also restores an actual origin snapshot. The physical receipt
regression now verifies that changing a finite job to None still creates no call
receipt. Actual guard enforcement additionally depends on the maintenance owner's
private-origin traversal and the integrity owner's source-operation attachment.
The 041 migration and canonical new-job hooks belong to integrity, migration
registration to surface. This group must be integrated coherently before Cargo;
no local Cargo or runtime validation was performed for this checkpoint.

The physical usage observer applies the same typed classification before SDK
callbacks erase storage and producer-guard error types. Original lease conflicts,
transient outages, configuration prerequisites and terminal corruption remain
distinct through callback wrapping. Explicit unavailable or invalid environment
credential references are repairable configuration prerequisites; invalid model
output and endpoint escapes remain terminal. Receipt creation/completion and
unknown-usage retention are unchanged.

Queue claim and renewal deadlines use checked positive duration and timestamp
arithmetic. Oversized configured timeouts return a typed configuration failure
before changing lease state; a runner renewal failure uses the same original-
attempt preflight transition as other configuration prerequisites. Regression
source checks both backends, both claim selectors, zero/negative/overflow values,
and unchanged original ownership after a rejected renewal.

### Dispatcher native fixture initialization

Native dispatcher regressions explicitly apply the complete migration sequence before creating records. Bare SCHEMALESS tables omit canonical epochs, receipts and validation events and cannot represent a configured service. The shared fixture now uses the real migration runner and creates a real named space before independently authored resolution assertions. Runtime startup remains read-only with respect to schema. These source repairs respond to the central runtime2 `canonical_schema_missing` failures; no local Cargo execution was performed.

### Immutable physical rendition identity

Accepted source materialization derives both the physical document ID and its unique rendition key from the original acceptance. Retained documents from replacement or a fresh A→B→A acceptance can coexist even when their logical template rendition keys match. Replaying one acceptance preserves its physical identity; the immutable input template is unchanged. Existing embedded/native replacement and ABA regression source now checks distinct rendition keys and actual replay. The embedded collision was observed centrally; the repair has only formatting/static validation locally.

### Resolution bundle dispatch

The exposed `commit_artifacts` dispatcher routes the complete resolution bundle set through `ResolutionPatchWorkflow::commit_resolution_bundle_batch` with the original live attempt. That maintenance owner validates the original decision epoch, source patch, run and bundle, then commits assertion effects, both artifact states, outbox and receipt in one canonical transaction. The generic staged-commit guard remains strict. Bundle creation retains the source artifact's complete policy/provenance envelope and stamps its own creation metadata.

Regression source uses the actual validation dispatcher and its generated commit child, a real producer claim/run and the original captured decision epoch. Both backends cover two-patch success, changed canonical input, and alteration of only the second bundle; refusal must leave every assertion and artifact uncommitted and the outbox unchanged. A type-size oracle checks the fixture without constructing its future. This batch depends on the maintenance owner's atomic bundle API and the integrity owner's producer bookkeeping epoch policy; no Cargo or passing-runtime claim accompanies it.

### Private producer intent fingerprint representation

Queue intent, reprocess intent and original child-key inputs are explicit named
versioned JSON objects, as required by the canonical fingerprint function. All
previously selected identity, policy, subjects, provenance, actor, payload and
original-run fields remain bound. This repairs the central runtime failure where
tuple serialization produced arrays and prevented ordinary work creation. Only
claim/runtime fields remain excluded from intent identity; sticky original budget
restriction remains separate private authority. The direct regression distinguishes
execution-state changes from changed input/authority/stages, while the existing
both-backend original producer/child tests exercise the active persistence path.
No migration is needed: the rejected tuple representation never produced a digest.

### Dispatcher negative-fixture authority

Resolution validation negatives now stage against a real assertion with real
revision/node evidence, then remove that assertion before invoking validation.
They retain the original rejection/no-bundle/no-enqueue assertions at their intended
boundary while respecting the staging writer's current authority check. The native
TM detection fixture creates the real relation endpoints required by the strict
RELATE path. These are fixture repairs; product staging and graph checks remain
unchanged. Central validation is pending; no Cargo was run in the owner worktree.

### Dispatcher poll stack boundaries

Each active work-kind branch is a separate boxed async phase. Future storage
size alone does not bound the stack used while polling: the frozen Runtime6
dispatcher reserved 1,412,016 bytes for branch temporaries in one poll function,
before nested phase calls. The boundary preserves the original shared source
and predecessor checks, attempt, work run, metrics and early-return behavior.
Existing actual resolution, source, and truth-maintenance dispatcher regressions
exercise these paths on ordinary test stacks; no global stack increase is used.
The source change still requires central runtime validation.

### Shared accepted-document projection and real producer fixtures

Accepted worker materialization delegates to `source_intake::stage_source_revision`
and reads the exact accepted physical document ID. Intake, recovery and worker
paths therefore share one rendition identity and immutable-input comparison;
the accepted logical template is unchanged. Both backend replacement and fresh
A→B→A regressions stage through intake before worker replay and all-space recovery.
This requires intake's matching `accepted:<physical Doc ID>` projection change.

Dependency regression fixtures now enqueue actual private successors from real
claimed work and original execution receipts. A completed work row alone does
not make its unfinished run successful. Reprocess continuation fixtures persist
the real restricted job, conflict group and run. The private-origin snapshot
fixture finishes its original claim before export, then checks restored ancestry,
child production, archive and missing-origin refusal through the existing paths.
These regression sources await centrally scheduled execution.

### M10 claim and retry scenarios

The retained M10 scenarios assert the actual unique lease token and durable
attempt fence, including refusal of a bare worker identity and a previous
claim's token. Backoff uses the production retry policy with a bounded one-second
test base, actual due-time refusal, a second real claim, increasing delay and
finite exhaustion. No test writes Running state or increments attempts manually.
The same scenario source is consumed by both existing test crates.

### Actual dispatcher fixture dependency waits

The shared binary dispatcher fixture persists the original failed WorkRun and
attempt fence before handling `PendingDependencies` with the production retry
policy and original-token blocked requeue/refund. It returns the original typed
error. Contact scenario callers independently check the failed receipt, refunded
count, unchanged graph and real minimum delay before retrying; other errors stay
fatal. No readiness proof is bypassed and no delayed work is counted complete.
Test dispatch entry points print only stage/work identity and begin/end markers
to locate an actual nested stack failure in the next central runtime gate.
### Source watch acceptance uses the original worker

The `refresh_source_watch` dispatcher passes the fence extracted from its captured claim and its existing WorkRun ID to `SourceWatchService::check_with_attempt`. The intake/storage owners validate those original identities, the watch and its source authority in the acceptance transaction. A quiescence activity guard or an unchanged source generation is insufficient to authorize a worker whose lease was reclaimed. This caller change depends on the coordinated watch-producer service/storage batch and its actual loopback fetch/reclaim regression source; direct authorized watch checks retain their separate service entry point.

### Original watch attempts at the real dispatcher boundary

The queued watch branch forwards the original claimed attempt and actual original
WorkRun into the watch service. The dispatcher regression child pauses a loopback
fetch, cancels or expires/reclaims the original lease without changing the Source
head, then releases changed, not-modified, or failed HTTP responses. Both storage
backends must refuse stale check success, preserve Watch/check state, Source/head,
immutable documents, original operations and successor queue ownership, and create
no orphan revision. Live original attempts still accept changed content and enqueue
its generation, or record a not-modified check. The network policy override exists
only in cfg(test) task-local scope and uses the existing bounded fetch policy.

These cases are authored for the coherent A/I/B/M watch producer group; they have
not been executed in this worktree. They require the service original-attempt
acceptance and record-check guards, and do not establish runtime qualification.

### Fenced watch acceptance-to-queue handoff

Fresh watch acceptances enqueue through `enqueue_accepted_source_from_attempt`.
The existing private-child queue transaction verifies the sealed original Watch,
WorkItem, WorkRun and live attempt, the actual accepted source-operation origin,
immutable source revision and child authority. It retains the accepted publication
fence and original private parent ancestry, including source references for erasure.
Only this checked source case preserves publication context; maintenance children
continue to strip unrelated source context. An obsolete parent yields LostLease.

Matching watch acceptance retries are receipt-only at the intake boundary and must
not call the enqueue mutation. Durable source recovery handles the accepted gap.
Four new storage regressions exercise valid original queue replay and cancellation
after durable acceptance but before enqueue, for both backends. They assert original
receipt/head preservation, no stale queue insertion, and actual Child ancestry.
The dispatcher also has an exact classifier oracle for the existing typed terminal
`InvalidState(accepted_contact_plan_unavailable)` outcome. Source-only checkpoint;
A `0f9e4b2` plus `9b30937` and I consumer/record-check guards are required to join.

The queued watch result distinguishes a recorded observation from a read-only
original operation receipt. The dispatcher logs the receipt's actual state without
creating a check or claiming publication completion. Intake must derive that outcome
from the acceptance transaction itself; a preflight absence check is insufficient.

### Accepted Watch gap recovery with original finite ancestry

Both backend regression sources now compose real finite reprocess ancestry,
Watch acceptance, cancellation before the initial queue handoff, and quiet
all-space recovery. The refused obsolete handoff leaves queue, Watch and original
acceptance unchanged. Recovery preserves the source's original Watch/run/attempt
producer. Changing the current job budget to unrestricted cannot remove its
original finite restriction: the real physical-dispatch guard still refuses.
The actual supported Metadata pipeline can then complete this same original
operation without a model call; the test requires its durable completion and
canonical receipt, never a queue-empty surrogate. These sources require the
coherent A/I/B/M Watch group and central execution; no runtime pass is claimed.

### Managed wiki original authority and partial progress

Queued reconciliation passes its original claim fence into the canonical wiki
planner. `capture_managed_wiki_grant` returns the exact active lock and grant rows
for that planner's original transaction read set, so revocation during planning
can invalidate the commit. Capture alone never authorizes a later mutation.
Both backend regression sources cover exact row capture, wrong-space refusal,
revocation of both dependencies and a separately authorized replacement grant.

The runner records partition and reconciliation failures alongside article
failures. A successor retains a successful graph fingerprint only when all three
are clear; partial committed progress cannot suppress the remaining work. This
source group depends on the canonical wiki planner and its original-read guards.


The pipeline supervisor classifies a completed worker against the actual shared
shutdown state. A clean worker completion and a ready shutdown notification can
win either select branch without becoming a false unexpected-exit error. A
false notification does not request shutdown. Worker errors and panics remain
failures, including every failure returned while draining requested shutdown;
an unexpected exit aborts and joins the remaining workers. Regression source
pins the both-ready join winner, error/panic aggregation, and orphan-free failed
supervision. These tests are authored and await central validation.


The retained runtime TM regression fixtures now derive partitioned slots through
actual queued detection, original resolution and atomic commit. They no longer
copy an obsolete security-only partition hash. Recompute/reprocess must enqueue
a new successor distinct from setup's actual history. Memoized replay first
executes and commits a real scripted original verdict, reads its persisted full
decision memo, then requires no additional commit and unchanged claims/group.
The fixture uses complete paginated queue inventories and real claim/run
transitions. Production partition filtering and memo fingerprints are unchanged.
These three retained regression cases await central Rust execution.

### Classified reprocess creation

`prepare_creation` captures an existing named SpaceConfig and returns the new
Pending job with that configuration's complete security, subjects, subject state
and provenance. The caller authorizes this exact job before passing the sealed
preparation to `create_prepared_with_work`. The existing atomic handoff transaction
rechecks the original configuration before creating the job, its private finite
producer evidence and the initial work item. A native same-row write fences a
concurrent configuration update; a changed or missing original configuration
refuses the whole creation. This prepared API never substitutes default policy
for an absent configured space. Existing explicit trusted creation APIs remain.

Both backend regression sources cover classified inheritance, original finite
origin, rejection after reclassification, and physical absence of all three
partial outputs. Source execution remains centrally scheduled.


Native reprocess queue recovery retains full original Job comparisons and writes
a real private nonce on the existing named-space canonical epoch row. Every Job
writer's migration031 event writes that same row, so an overlapping changed Job
cannot pass a stale recovery snapshot. The guard leaves Job fields and the
semantic epoch unchanged; an uninitialized epoch fails explicitly. Same-value
Job/config timestamp updates are removed. Classified creation already creates a
Job in the guarded transaction, whose epoch write conflicts with configuration
writers. Native regression source checks distinct persisted guard nonces with
unchanged job, queue identity and semantic epoch; actual overlapping-writer
qualification remains in the central independent concurrency gate.


### Managed wiki queue identity after archival

Managed wiki enqueue now checks global physical archive presence in the same
transaction before creating a hot Work item or touching its grant. An archived
original cannot be recreated by replaying a same-intent proposal or supplying
its ID from another space. Actual cold-to-hot recovery continues through the
existing guarded retention path; ordinary new successors retain fresh IDs.
This closes the remaining ordinary queue-create writer omitted by the generic
Work/Run and child/reprocess archive checks. Grant and registration-lock rows
remain excluded from terminal archival. Two backend regression sources create
real grants and work, archive the terminal original, refuse both duplicate
proposals without changing original authority/history, and admit a fresh job.
These source tests have not been executed by the owner.

### Explicit standing Watch authority (source group, not yet runtime qualified)

A recurring registration is a deliberate durable delegation. Its JWT expiry
limits ingress registration; subsequent checks retain the original principal
ceiling and intersect it with the actual current named-space grant. Missing,
revoked or replaced authority refuses. A broader later grant never broadens the
original clearance, subject, purpose or scope ceiling. There is no legacy
backfill or actor-string inference.

`StandingWatchAdmission` is C's non-serializable verified/trusted constructor.
B persists a private `watch_delegation` descriptor only through I's atomic
Source/Doc/Watch readset plus registration transaction. Migration 042 gives the
row typed source/watch references, an independent canonical epoch event and the
same physical-erasure reference fence as other private producer rows. C owns the
registry/checksum join; E owns the complete physical inventory/purge consumer.

Each queued scheduling slot binds the exact delegation ID/digest, semantic Watch
intent and durable `next_check_at`. The guarded queue transaction checks the
original records, the standing owner and an optional independently authenticated
manual invoker. Same-slot reactivation uses existing hot/archive lookup and cold
promotion, retaining original Work identity, private origin and historical runs.
A new genuine claim rotates the durable fence; an earlier token cannot complete
it. There is no archival exemption or time-derived retry UUID.

Explicit local synchronous convenience also uses real queued work, a real claim
and an actual WorkRun. `prepare_local_registration_if_absent` can establish local
standing authority only at a guarded absent row; existing local registration
requires the identical selected ceiling. Existing signed service registration
refuses local adoption. Missing authentication never chooses a trusted origin.

Accepted Watch operations carry the original producer into descendant writes.
`PreparedStandingSourceAuthority` walks bounded original execution edges and
retains raw operation/private-origin controls and full hot/archive Work/Run
observations. Its committing guard checks every applicable standing delegation
and exact original evidence, including a Direct classification. It exposes all
applicable principals for effect authorization. Source selection/content history
alone is not an execution-authority edge. The independently authorized job root
and background inherited-job distinction is a separate required producer join;
this storage checkpoint alone does not claim that extension is complete.

Authored regression source covers signed grant broadening/revocation, current
configuration races, explicit replacement refusing old queued identities,
original archived-slot reactivation, obsolete-token refusal, and guarded local
registration racing a signed registration on both storage
backends. These Rust tests have not been run in the owner worktree. The group
requires coherent C/I/A/F/E consumers and the inherited-job extension before
central validation; no service/provider execution is authorized by this source.


Private reprocess roots now record a required `execution_origin`: an explicitly
admitted independent job, or the exact Work/WorkRun/Attempt and immutable parent
intent which spawned it. Canonical callers capture `PreparedReprocessParent`
after the read-only receipt replay path and persist the job and origin in the
same transaction. A recovered queue handoff preserves this original descriptor.
Selected content history does not create an execution-authority edge. Inherited
jobs retain ancestor budgets and standing Watch ceilings even when their own
budget is absent; independently admitted jobs retain their own authority.

Old private descriptors lacking this required field are refused as unknown
original authority. There is no automatic backfill, actor-derived admission, or
generic migration. Authorized new job admission/reconfiguration is required.
Original parent receipts remain usable after lawful archival, but an ended
parent attempt cannot admit a new job. Original operation/private controls and
all consulted historical rows remain in the guarded transaction read set.

`PreparedStandingSourceAuthority::capture_for_work` and source-operation capture
share one bounded ancestry resolver. `source_ids` identifies the actual discovered
inputs; `authorize_record` applies the registration policy to a concrete
Source/Doc/revision/node/artifact under every applicable original principal and
retains that exact record for `verify_current` and mutation guards. Consumers
must supply the actual affected inputs; a valid grant alone is not record ABAC.
Embedded authority/history reads use read-only storage access rather than a
mutation closure that might checkpoint the managed store. Native private control
reads guard their unchanged payload with a real changing nonce.

Regression source covers both backends: inherited job creation and the durable
job-to-queue gap, original archived parents, independent admission, missing
private admission, stale-parent rollback without partial job/queue/control,
work-seeded standing capture and record denial, and current grant revocation.
These tests are authored for the coherent A/B/I/M standing-authority join and
have not been executed in the owner worktree.


Standing execution ancestry retains the existing 256 original Work limit. Source
operation observations are counted separately (at most one per Work plus the
entry operation), so a Work's publication link does not halve supported ancestry.
Cycles and exhausted complete traversal still refuse without partial success.
Exact affected input records use ordered identity deduplication, not a queue-depth
multiplier or a first-N selection. The prior 1,024-row idea is withdrawn: a
supported 2 MB source can already exceed it. Consumers must select the complete
actual source/manifest population or exact physical-call inputs and retain all
of them. The final I/F input population/resource derivation and independent
large-document oracle remain required before this held family is qualified.


The final aggregate input API is `authorize_record(original_record)` with no
caller-selected scopes. I's sole `authorize_standing_source_input` maps Source
to existing source read/write capabilities and Doc/Revision/Node/WorkArtifact
to existing document read/write capabilities. Every original principal is checked.
Proposed after-images use that same pure policy helper without adding them as
original stored read dependencies. No separate artifact capability is introduced.


The B-owned Watch dispatcher/recovery/storage regressions now establish an
explicit typed trusted-local delegation through the actual Source/Doc/Watch
registration transaction. Their original queued payloads carry that exact saved
identity, including the finite-job Child in the cancellation-to-recovery oracle.
No constructor, actor string or missing delegation supplies authority. Native
storage fixtures explicitly apply the same 042/043 schema. The existing reclaim,
cancel, changed/not-modified/unavailable, accepted-gap and finite-budget assertions
remain. Pipeline test-only auth/policy dependencies add only existing lock edges;
no resolver, new package, model probe or Rust test was run in this worktree.


### Original ancestry control payload consistency

The shared standing source/Work resolver validates the exact retained private
origin envelope, including its version, space, Work ID and immutable intent.
This also applies to original claimed and pre-handoff reprocess jobs. A second
lookup with the same origin enum cannot validate different earlier bytes.
The final transaction still compares those original bytes. Native origin guards
change the existing `read_nonce` field excluded by migration 041's semantic
predicate event; operation guards retain their existing control nonce. Neither
path manufactures a missing control. Source regressions cover changed envelope
identity with an unchanged origin and two actual native guard commits preserving
the original payload and semantic epoch while changing the persisted nonce.
These tests are authored for the composed integration checkpoint; no owner Rust
execution is claimed.


### Read-only physical control existence

Standing dispatch absence observations use one exact physical control-key lookup
without a space filter. Embedded lookups hold the kernel state read lock and
storage import barrier; they do not enter a mutation closure, scan pages or
trigger a checkpoint. A foreign-space row blocks physical absence even though
a scoped payload read returns no row. Existing scoped private-control reads now
use this same point lookup with explicit space validation. The native adapter
selects only the exact physical record ID. These observations remain distinct
from authorization and the original same-transaction mutation guards.
Two authored backend regressions cover absent/present/deleted physical keys,
foreign-space presence, adjacent-key and prefix separation, and unchanged
original control contents. Rust execution remains with central integration.


### Original render Work through section summaries

The active RenderDoc dispatch passes its exact admitted Work into the document
tree builder's explicit execution overload. Optional section-summary calls
therefore retain original independent or inherited execution authority instead
of deriving authority solely from the selected source's history. The grounding
builder captures original input rows before tree writes and composes its fixed
input guard with the original provider observer and accounting. The existing
direct library entry point retains its explicit source-operation behavior.
This caller seam depends on the grounding and provider observer checkpoints;
it is not a standalone runtime qualification or an executed test result.


### Original reference consistency before ancestry deduplication

Converging branches may share a successfully verified operation or Work without
consuming another distinct-node slot. Every source reference must first match
the full originally observed source/generation/revision/operation fence, and
every Work reference must match the exact original snapshot. Matching only an
already completed ID is insufficient. Active-path cycles still refuse; the
limits remain 256 distinct Work and 257 distinct source operations. Two backend
regression sources use actual source acceptance and direct queue creation,
exercise 300 identical references without overcounting, and reject inconsistent
references encountered after successful original verification. No private
producer or completion rows are fabricated; Rust execution is not claimed.


The source publication repository exposes a narrow `head_exists_globally`
boolean for original legacy-input absence checks. It computes the existing
space/source head key, validates the requested identity and observes physical
presence without decoding or disclosing the control payload. A scoped missing
head is insufficient when that physical key contains a foreign or malformed
row. Existing two-backend read tests now cover that distinction and reject
invalid source IDs and blank spaces. Grounding/media consumers retain the
original absence observation across their fixed input checks.


The active media renderer uses the same explicit original-Work carrier through
`execute_render_doc_for_work`. The grounding wrapper and renderer observer
retain their ownership of exact document/source mapping and original media
input observations. This caller passes the original claimed item unchanged,
including its private producer reference and attempt. It does not infer new
independent authority from selected content or substitute a later Work claim.
This source seam is held with the grounding/media/observer family.

The work-queue configuration now retains its exact `doc_tree` recipe. Worker
construction reads the existing summarizer-model override once; render dispatch
uses that retained configuration rather than rebuilding defaults or rereading
the environment. Defaults and summary creation remain unchanged. Runtime capture
can serialize the actual `pipeline_config.work_queue` passed into the managed
worker, including retry limits and tree configuration. Capturing all of
`PipelineConfig` is inappropriate because it also contains database credentials.
The production render fixture asserts that a retained non-default paragraph
setting controls the persisted tree; the config regression preserves the exact
recipe through serialization. These are authored Rust tests, not executed
validation evidence. The API owner controls private capture and embedding retry
policy remains the provider owner's separate observation.


The reprocess-creation stale-snapshot regression uses a supported guarded
configuration-purpose update after preparation. Ordinary SpaceConfig replacement
preserves the original security envelope, so attempting reclassification in
fixture setup fails before reaching the intended job/origin/queue transaction.
The test still requires the original prepared configuration to refuse the later
snapshot and leave no partial job, private origin or queue handoff.

The finite-job physical-dispatch regression applies the full explicit schema to its SurrealDB memory fixture before creating the original job/handoff. This preserves the required reprocess epoch guard and reaches the currency refusal and zero-receipt assertions. Serving remains schema-read-only.

Watch regression fixtures establish the same prerequisites as live dispatch:
original source acceptance precedes standing registration, replacement names
that original generation, and queued private execution retains the registered
creator rather than a scheduler log label. Replay no-mutation comparisons inspect
all persisted typed and private rows under the transaction view without writing
or completing the original lease. They separately assert backup export still
refuses running work. No production authorization or snapshot guard is relaxed.

The source-intent persistence regressions retain nonempty contact/manual-document
metadata and differently ordered nested objects through actual WorkArtifact
repositories on both backends. A stored recipe must still equal its original
fingerprint after decoding; array order, scalar changes, classification and
original source timestamps must remain distinguishable. These controls depend
on the canonical object-order repair in maintenance's request fingerprint and
do not migrate or rewrite earlier stored authority.

Standing watch admission and source recovery compare the original saved space
policy using one projection. It excludes only bookkeeping `updated_at` and
storage-owned `processing.index_generation`, which the same accepted content
writes advance. Configuration identity, security, subjects, lifecycle, processing
generation, pending reindex, profiles, connections, bindings, purpose and skill
remain exact. The original delegation/grant, source, watch, attempt and native
transaction read fences are unchanged. Real content-write regressions cover both
backends and require a subsequent policy edit to refuse without a watch write.

Storage recovery fixtures explicitly install the real publication, queue and
private-origin schema plus the existing private/physical guard installer before
writing original records. They do not synthesize epochs or empty guard functions.
Standing convergence declares its named space before source admission; the
registration race changes and restores policy through original-row CAS without
refreshing the stale proof. Quiet-space recovery still has no SpaceConfig, so
its all-space discovery assertion remains independent of request traffic.

The common standing-policy comparison separately retains the original
`SpaceConfig.base.id`: the database CONTENT serializer intentionally omits record
IDs. Native SQL keeps its existing original-ID guard; embedded and source-chain
comparisons require the same identity. A pure regression changes only the ID and
requires refusal, while timestamp-only bookkeeping remains permissible.

The accepted-bytes Watch recovery regression compares every persisted table
while the new original attempt remains Running. It now separately asserts the
real observation transaction's three conflict-control effects: a fresh attempt
commit nonce, a rotated canonical epoch, and exactly three erasure-guard revision
increments (attempt touch, check create, Watch replacement). Only those asserted
fields and the actual Watch/check records are normalized for comparison. Source,
head, receipt, document, queue, original lease and every other private control
field must remain byte-for-byte equal. Runtime17 exposed these control writes;
its original failure remains retained. The corrected original regression passed
in central Runtime19.

The original all-stage reprocess dispatcher regression retains its empty source
selection, divergent slot, unresolved group and newly enqueued resolve-child
assertions. Requested ReMaintain work now requires a typed PendingDependencies
outcome while those real children remain. The same durable WorkItem must be
Runnable with its lease released, execution-attempt budget refunded and an actual
future due time; its original WorkRun remains completed/unsuccessful with the
same typed attempt fence and diagnostic. The same all-stage job must remain
Paused without a completion timestamp. No requested stage or group assertion is
removed. This fixture integration depends on maintenance's guarded source-job
completion repair. The original dispatcher regression passed in central
Runtime19 (1.826 seconds).

Runtime19's unchanged native ancestry boundary timed out during actual successor
construction, before its first 256-Work traversal: 32 Works took 10.472 seconds,
64 took 44.487, 96 took 111.676, and 128 took 220.915. Admission still checks every
original ancestor. It now reads each next Work, original WorkRun and private
origin together, using the existing storage proxies and hot/archive projections.
The prefetched origin belongs only to that one observation; the complete rows
and original envelope remain inputs to the committing transaction. There is no
cross-request cache, inferred producer, new schema, or reduced ancestry bound.

Native admission generates every row guard through the existing canonical
`append_row_guard` function. Consecutive guards with identical SQL and binding
names share a descriptor loop; every original descriptor, its position and
multiplicity are retained. The unchanged template performs the hot/archive
selection, complete field comparison and conflict-nonce write. Original private
origin guards use the same template approach, retaining their exact space and
payload comparisons and nonce writes. All attempt, row and origin guards still
execute in the same transaction before the child or its origin is written.
Changed or missing originals must refuse the transaction and roll back earlier
guard writes. Runtime19 establishes the construction bottleneck, not a measured
speedup: the complete source successor must pass the original 256-Work/257-Source
control under its existing 240-second deadline in the central validation lane.

The native batching regression module uses actual direct queue admission,
claims, original-fence WorkRuns and the real history archival path. It checks
hot/cold read parity and duplicate descriptors, then changes originally absent
Work/Run fields, native numeric types, private origin payload/space, and cold
archive envelopes. Each refusal must leave the later sentinel absent and roll
back conflict-nonce writes made by earlier valid guards. Missing originals and
wrong-table tuple identifiers also refuse. These are authored source controls;
their runtime outcome belongs to the next coherent central gate.

Successful accepted-source triage retains its actual decision identity in
`WorkRun.metrics.custom["source_ingest_decision_id"]` as the original RecordId
string. The existing attempt completion persists the returned metrics on the
same successful WorkRun alongside its original attempt fence. A downgraded
accepted mode refuses before setting this field; any later dispatch failure
does not return successful metrics. This carry makes no new decision, query or
Source progress mutation. Publication proof can resolve the exact retained
decision rather than selecting a later decision by timestamp. Native and
embedded dispatcher regressions read that actual decision, compare the original
completed run and attempt, and require Source.latest_ingest_decision to remain
unset until the separate canonical publication step.

Runtime21 passed all five native guard-vector controls and the embedded original
ancestry boundary. Native construction improved (128 Works in 112.747 seconds,
versus 220.915 previously) but still timed out at 240.725 seconds after reaching
160 Works. The boundary remained unproven. The next observation path collects
only original Work IDs through at most 256 private-origin hints, then reads all
original private payloads and the bounded hot/cold Work and Run targets. Hint
rows are never authority: native ID types, physical private string keys, spaces,
parent Work/Run links, exact ordered payload-derived chain, genuine terminal,
missing records and cycles are checked before admission. A nonterminal at the
256-step limit refuses instead of accepting a truncated chain.

The existing Rust dependency walk still verifies every full Work envelope,
original parent/run proof and actual independent or inherited reprocess job.
Its emitted row/origin order and multiplicity are unchanged; the previously
validated transaction guard templates still compare every original dependency
before child publication. The temporary observation maps last only for this
admission and add no stored state or authority. They reduce client round trips
and repeated full projections; further speedup remains unmeasured until the
original native 256-Work/257-Source case passes its unchanged central deadline.

Runtime22 exposed a source handoff gap: the real TRIAGE run recorded its decision,
but the metadata work was admitted as an independent Direct producer. Its public
predecessor ID carried no original execution receipt, so the publication proof
could not recover the real decision. Accepted TRIAGE-to-metadata now uses the
source-specific producer factory. It validates the exact original publication,
configuration, plan and source identity, inherits the public envelope, and
retains a private Child edge to the original parent Work/Run/attempt in the same
guarded enqueue transaction. Metadata waits for that original execution to
succeed. The generic maintenance sanitizer remains unchanged.

Only the three validated source context fields survive the dedicated factory;
attempt, bare predecessor and metadata-bundle fields are removed. The private
origin explicitly retains the accepted Source reference for erasure inventory.
The configured triage and publication/plan identities stay unchanged. Metadata
handoff identity is bound to the original run, making exact same-attempt replay
stable while preventing a later retry from adopting a failed earlier execution.
Other stage enqueue and manifest/bundle/recovery identities remain unchanged.
No existing Direct history is rewritten or treated as a certified Child.

The two actual dispatcher regression definitions now verify the private original
edge, successful metadata execution and its actual artifact's retained source
context. They also exercise real live-claim handoff/replay, predecessor blocking,
substituted source/config/plan, changed original intent/run and stale completion
refusal without changing original Work/Run or admitting additional metadata.
The actual toy-flow non-null decision oracle remains unchanged. These additions
are source-only until the next coherent central validation.
The handoff controls additionally perform a real retry after the original run
fails following child creation. The new claim gets its own stable child while
the old child's private edge remains byte-equivalent. Completing the new run
releases only its own child; it cannot satisfy the failed earlier child's
predecessor. Both remain discoverable through the existing kind/status query,
and their original publication/configuration/plan payloads agree.
