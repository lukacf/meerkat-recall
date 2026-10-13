# Source publication transactions

Source acceptance and source publication are different durable transitions.
`SourcePublicationRepository::accept` stores the source-first input, immutable
source revision, desired head, and accepted operation together. The head's opaque
generation orders accepted intent, including a replacement still in progress.
A content hash, observation time, or published revision is not a CAS token.
Repeated observations do not alter immutable input or create a new operation.
Idempotency also binds the logical source identity, including sources without an
external identifier; a key cannot silently replay an unrelated source request.
Changed text, classification, or semantic metadata under the same revision is a
conflict. `observed_at` is excluded from the immutable request fingerprint.

Every existing head requires its exact generation. Retraction requires an explicit
`reactivate` request before new input may revive that incarnation. Erasure requires
`recreate_after_erasure` and allocates a new physical source ID. A payload-free
external-identity pointer selects the new incarnation; old erasure journal entries
continue to target only the erased physical ID.

The minimal source policy envelope is retained separately from source payload.
Operation authorization uses the envelope accepted for that operation, including
after erasure or recreation; it never adopts a successor incarnation’s policy.
The envelope contains security and subject-scoping metadata, not text, external
identifiers, metadata, titles, or source hashes.

`CanonicalRepository` commits complete typed effects, outbox events, and a durable
receipt in one database transaction. Both the embedded kernel and SurrealDB use
the same semantic checks. An existing assertion is not a completion receipt.
The receipt binds the immutable input fingerprint and exact prepared effects,
dependencies, outbox, and source authority. Reusing a receipt identity with changed
semantic input fails explicitly.

Preparation reads the per-space canonical epoch before reading records or
selectors. Every competing canonical write advances that epoch, including direct
repository writes, new identity matches, support changes, and hard deletion.
Publication compares and advances the epoch in its transaction. This deliberately
conservative mechanism fences empty matches and changed membership as well as
edited records. A conflict requires fresh preparation; retrying a stale plan
cannot silently overwrite the winner. SurrealDB requires migration 031 and its
complete event set; publication fails closed without the schema marker. Snapshot
restore rotates preparation epochs before ordinary readiness resumes.

Source publication additionally checks the original queue attempt, desired head,
space, and sealed publication requirements. Queue retry counts are not lease
authority. The dedicated attempt generation never resets and its claim token is
unique. Source-backed node bundles remain staged until the complete expected set
and required search coverage are ready. One finalization transaction publishes
the revision, retires obsolete exclusive support, writes all effects and events,
and completes the source operation. Metadata or individual artifact completion
cannot claim that a graph revision is searchable.

Maintenance of already-published support uses a separate captured support context.
It checks current source generations and published revisions without pretending
to publish a new source. Shared baseline records cannot acquire private semantic
fields without contribution-level support; source-partitioned records preserve
independent knowledge when a source is withdrawn. Evidence additions remain
explicit provenance and must be reconciled when their source is removed.

Permanent payload-free erased-reference fences reject delayed staging writes,
including an extractor that resumes after its source and work run were erased.
The embedded kernel applies the guard on both direct and transaction mutation
paths. SurrealDB applies corresponding mutation events. Erasure control receipts
contain opaque identities; source content is not retained in those guards.

Cross-source maintenance decision reversal and physical erasure are coordinated
with the maintenance and erasure modules. The acceptance/canonical checkpoint
alone does not certify those integrations. Existing guarded lifecycle and
temporal correction remain supported requirements.

Regression source covers desired-head races, immutable retries, independent
spaces, predicate phantoms, transaction rollback, and delayed erased artifacts.
Fault hooks run inside the real canonical transaction after the first effect,
before outbox, and before receipt. Authored tests and formatting checks are not
execution or deployment qualification. Backend isolation, complete active-path
integration, journal/restore behavior, and indexed source visibility must be
validated against the joined implementation and its actual runtime artifact.

Authorized mutations carry `SourceAuthorizationSnapshot`, containing the evaluated
policy envelope and observed source version/deletion marker. Acceptance compares
this pre-state inside its transaction. An authorized new revision can reclassify
security or subjects; a policy change after authorization causes a conflict.

Multi-source maintenance records a causal inverse receipt with the complete
before/after effects and a transactionally allocated decision order. Every input
is required for a derived decision. Source withdrawal prepares inverses in reverse
order, including decisions that read an invalidated output. It restores baseline
fields only when they still equal the invalidated decision’s output, preserving
later independent edits. The lifecycle transaction applies this plan before source
lineage pruning, and deletes decision snapshots and successor replay receipts.
Preparation itself performs no live mutation. Physical cleanup integration and
transitive invalidation require the lifecycle owner’s joined source and tests.

Canonical receipts include the exact successor output needed to resume after a
commit-before-enqueue interruption. This output is hash-bound to the plan and has
an explicit union of contributing source IDs. It is payload-bearing and must be
physically removed if any contributing source is erased.

Source support binds the exact canonical row with a semantic fingerprint; only
`updated_at` is excluded. Reads compare the proof to the current row, and canonical
mutation validates input proofs before refreshing support for its validated
outputs. A direct write cannot reuse an old grounding proof for changed semantics.
Lifecycle inverse and shared-lineage cleanup refresh surviving support in their
own atomic transaction. Receipt payload replay also rechecks current source
lifecycle, generation and published revision, without requiring a completed
worker’s old lease to remain live.

Worker progress and typed failure states are written under the original source
and attempt fences. Stage outcomes cannot mark a source complete. Work artifacts
are staged control state: exact row guards and sealed expected sets protect them,
while their creation does not invalidate the canonical epoch carried through an
asynchronous decision. Semantic tables, source/config/support controls and index
coverage remain part of the coarse predicate fence.

The kernel's `tx_with_commit` exposes the final physical row delta to a storage
commit callback while the write lock remains held. Callback failure rolls back
rows, indexes and epochs. The managed storage owner must fsync a complete framed
log entry before acknowledging that callback; ordinary in-memory `tx` is not a
persistence guarantee. Live log replay accepts committed Running leases for queue
recovery, while ordinary backup import continues to require quiescent work.
The live payload log must be physically compacted after source purge before
erasure completion. Kernel callback tests do not replace process-kill/reopen tests
of the storage owner's filesystem protocol.

Source replacement seeds the promotion overlay with the same causal inverse and
shared-lineage reconciliation used by source withdrawal. It then plans the new
revision against that surviving baseline and publishes both in one transaction.
The canonical boundary independently recomputes the required retirement effects;
restoring another source’s baseline does not give the replacing source credit
for that content. Decision-created rows cannot become independent merely because
a later source writes an already-existing row. An altered new decision output
without a certified independent baseline keeps cleanup pending rather than
silently deleting later independent work.

Declared extension rewrites in identity merges now join the same canonical plan,
transaction, fingerprint and receipt as typed rows, including `IdMap` and
`ReprocessJob`. Native extension values retain their database types. Causal
receipts store exact extension before/after images; withdrawal reverses nested
source-derived leaves while preserving independently edited siblings. Ambiguous
concurrently edited arrays remain a cleanup conflict. Replacement and erasure
consume those inverse effects at their existing atomic boundary. Declared table
fences must be installed before the first merge read, including empty selectors;
embedded extension writes intrinsically advance the same space epoch. Material
unchanged extension reads use a before=after effect to retain causal dependencies.

Background source acceptance can bind both the original `AttemptFence` and a
captured `SpaceConfigToken`. Both are checked inside acceptance, and native
configuration reads participate in its write conflict. An identical old receipt
can still be read; stale work cannot accept a new head. This does not implement
unpublished manifest reconfiguration: the manifest owner must rotate its recipe
and staged identities under the original source fence before resumed dispatch.

Regression source covers mixed typed/extension rollback with outbox and receipt,
IdMap/ReprocessJob native round trips, recursive extension causal inverses,
manifest/extension epoch and erased-reference guards, and acceptance after a
changed configuration or stale claim. These definitions are authored; this lane
has not run Cargo or claimed backend/runtime qualification.

Trusted direct reprocessing binds the same token through
`accept_with_configuration`; queued paths additionally bind their real attempt.
Reprocessing admission can query `operation_by_idempotency_key` to count actual
previously accepted intents without duplicating storage key derivation or using
job progress as a second operation authority.

Embedded extension numeric string identities and integer identities now use
distinct physical keys. Supported raw string and `{tb|table,id}` string forms
normalize numeric strings to a quoted native string key; integer keys keep their
integer identity. Snapshot reconstruction and durable delta replay use the same
identity check. This is independent of the pending non-UUID late-write erasure
fence: a public salt and substring matching were rejected in source review and
are not a delivered privacy guarantee. Exact typed reference normalization and
private original-store key continuity remain required before qualifying that
additional erasure seam.

Source head, support, revision mapping, completion and active manifest changes
advance the existing processing index generation in the same transaction. This
invalidates staged reindex coverage when eligibility changes without a text edit.
Manifest history is covered conservatively. A transaction-only fence nonce with
unchanged payload/state does not advance that generation, so vector pages can
complete their own source checks. Both-backend regression source checks create,
changed visibility, deletion, rollback and nonce-only writes; it is not executed
qualification. Migration 038 adds native events without changing prior schemas.

Native compare-and-swap uses transaction FOR checks over expected row fields. Nested expected objects use native persisted NONE semantics while top-level absent fields remain explicit checks. Source-decision ownership comparisons run outside isolated Surreal closures, preserving independent-baseline classification.

Independent public edits cannot refresh old source credit for a changed Rel,
Attribute or Assertion factual signature using the same documentary passage.
Confidence, status and nonfactual annotations remain editable. Clearing evidence
or copying a passage into a fresh assertion ID does not create new proof. An
existing resolved assertion may support a same-passage correction only when it
is an observed dependency, its tuple matches, its source support fingerprint is
current, and its published revision is among the captured source fences. Public
typed validators also check new citations; this guard does not infer entailment
from arbitrary quotation text. Explicit source-less human authoring remains
available. Entity summaries and Event descriptions are outside this bounded
factual-signature check and are not claimed as newly semantically certified.

A content-hash revision may be accepted again after another revision. Reusing the
original idempotency key remains a historical receipt replay and never advances
the head. A fresh explicit key plus the exact current generation creates a new
acceptance operation/generation while reusing the unchanged immutable payload.
The original revision snapshot and its first acceptance time remain immutable;
the new operation records its own acceptance time. Changed semantic content under
the same revision still conflicts. Physical document, revision, work and manifest
identities must include the returned acceptance fence; an older document mapping
cannot be rebound to the new operation. Completion and configuration replan use
the exact acceptance instance, never revision-string equality alone.

Both-backend acceptance regression source covers original-key historical replay,
fresh-key A/B/A, stale CAS, two concurrent copies of one intent, two competing
fresh intents, unchanged immutable storage and same-revision stale-attempt
rejection. Execution remains with the scheduled integration/qualification lane.


Private reference fences use the pinned SurrealDB 2.6.5 reference-token builtin,
version 1, for both engines. The storage adapter supplies the original independent
journal identity and its private 32-byte key. Native table events inspect the
actual postimage, including partial updates and expression results; the embedded
kernel calls that same native normalizer through a runtime-only adapter. Tokens
use standard HMAC-SHA256 over typed, space-scoped reference identities. Native
string and integer keys remain distinct, and incidental prose is not a reference.

Migration 039 replaces the former substring events and installs the shared native
function. Declared and restored historical extension tables receive the same
guard. Each ordinary native create, update, or delete advances a checked per-space
guard revision, including queue and artifact tables. Erasure captures the revision
before inventory and compares it inside purge; this also detects a remote write
that committed entirely between those phases. The shared row establishes a write
conflict for overlapping transactions. Journal membership must be durably recorded before
physical purge, then projected in that same purge transaction. Guard counts reject
incomplete membership replay; missing or mismatched runtime key custody fails
closed. No key is serialized into kernel state or ordinary snapshots. Ordinary
kernel backups omit membership tokens while retaining the anchor/count; private
live checkpoints keep the projection. Restore needs the original journal's key
and complete membership before readiness.

This requires the paired patched remote server and capability handshake. The
client's parsed import restriction, independent key/membership durability, purge
integration, restore barrier, and remote artifact are separate owner joins; the
presence of migration 039 alone is not a qualification claim. The regression
source exercises native evaluated partial updates and preceding-statement
rollback, embedded transaction rollback, string/integer separation, encoded JSON,
space/key separation, and missing-key or incomplete-backup replay refusal.

Erasure reserves that shared guard for the original durable journal intent before
inventory. Every ordinary writer in the space then receives a retryable conflict;
only the trusted maintenance scope for that exact intent may mutate it. The
reservation has no timeout. This prevents independent ownership changes while
membership is fsynced outside the database. Purge checks the captured revision,
preserves independent after-images, installs membership and clears the reservation
in one transaction. Failure leaves the reservation for original-intent recovery.
Recovery resumes reserved intents first and fails closed if the original journal
cannot identify the owner. The extra private query context is never sourced from
public request metadata. Composite deleted identities use identity-only HMAC;
ordinary postimages still recursively discover references inside composite keys.

The embedded kernel seals the journal-qualified reservation permit once when a
transaction is admitted. Any reference guard, reservation, or revision-capacity
failure poisons that transaction: catching the mutation error and returning
success cannot commit earlier payload, index, or epoch changes. All kernel
mutation paths use the same rollback boundary when private fencing is active.

The `source.erased` event label is not an erasure-fence bypass. Only a transaction
with the captured original journal erasure permit can emit a completion event
without checking its affected opaque IDs against the erased membership. Ordinary
producers receive the same full reference check regardless of the event name.
Reservation and revision checks still apply to that trusted completion event.
The caught-error rollback oracle also covers moving typed and extension rows
*out of* a reserved/exhausted original space, including extension upsert, and
requires the specific reservation or revision-capacity error.

Retained structured contact intake is an explicit accepted capability. The
source transaction persists its private `source_contact_intent` together with
Source, immutable revision, desired head and operation. An opaque digest on the
operation binds the original attachment. Ordinary metadata cannot recreate that
authority. The new table is payload-bearing, receives canonical epoch and private
erasure reference guards in migration 040, and is physically purged with its
source incarnation. No contact content enters the external erasure journal.

New contact acceptance compares the original epoch, complete row/absence images
and identity-key/company/relationship query sets inside the transaction. These
checks precede every source/configuration mutation. Identical retries compare the
original source semantics and capability presence first, then return the original
seal for adapter authorization. They do not run new contact selectors or replace
original authority because contacts have subsequently changed. Generic and contact
acceptance cannot exchange one scoped idempotency key. JSON object insertion order
is not part of immutable source semantics; array order and scalar values remain
significant. Explicit erasure recreation previews a deterministic fresh physical
incarnation derived from the old incarnation, space and new scoped operation; the
actual acceptance still checks the old head. A caller prepares contact authority
against that preview while retaining its original acceptance request identity.

Finalization prepares retirement once, appends the sealed contact effects, then
resolves graph extraction against that same overlay. Canonical publication loads
the operation and exact attachment again, verifies all original dependencies,
effects and contact outbox entries, and rechecks the complete original selector
results inside the committing transaction. Omitted effects or changed inputs
cannot yield a completed source. Additive graph promotion must retain the contact
contribution and tighten or retain its authorization envelope. A sealed contact
may change an independently existing record only when its original-before and
captured surviving retirement baseline match exactly; unrelated effects retain
the ordinary shared-record checks. The existing atomic decision receipt records
those field changes so source replacement/erasure can recover the independent
baseline. No public arbitrary effect-plan exception is introduced.

Maintenance may recognize a structured `works_at` fact without inventing a text
Assertion. `accepted_contact_rel_support` requires the original seal, current
published source operation, exact current Rel/support fingerprint, completed
manifest and canonical receipt containing that Rel. Its caller must still capture
the outer plan epoch, exact Rel snapshot and published-source fences. A source
provenance link or mutable `metadata.contact` alone is insufficient.

Contact qualification remains separate from source completion: A authors both
backend acceptance/retry/atomic selector fault tests, I authors the sealed plan
and baseline tests, and F owns the actual accepted-contact plus Graph finalizer
oracle. None of these tests was executed in the integrity worktree; centralized
validation must join the complete A/I/F/G/E/C group first.

Source operations also retain an explicit private producer origin. New ordinary
acceptances store `Direct`; a saved reprocessing acceptance stores the actual
job, selection and source-intent identities, closed-selection digest, exact
intent fingerprint and original admitting attempt. A finite currency restriction
observed at acceptance is sticky. Identical replay retains the original producer,
claim and restriction even if the current job later has no budget. Generic
acceptance cannot replay a reprocessing key as direct intake. Missing historical
producer metadata decodes as `UnknownOriginal`, never as unrestricted direct work.
These fields are absent from public operation receipts.

`PreparedSourceProducer::from_verified_reprocess_selection` captures the real
job/manifest/partitioned-intent snapshots after the coordinator verifies its
complete selection under an epoch captured before those reads. The source
transaction compares that epoch and all three complete records before mutation,
and binds the saved immutable source, expected head, original authorization,
recipe identity and processing configuration. Contact preflight follows the
selection write and uses the saved source and original authority. Its independent
snapshot and selector checks remain in the same acceptance transaction.

The source-origin record is an accounting restriction, not an authorization or
pricing certificate. Physical dispatch must also verify the job's immutable
original queue origin, current job restrictions, referenced saved selection and
current source/attempt. A missing referenced row or unknown queue origin refuses
dispatch. This prevents a current `None` job budget from overriding an earlier
finite restriction at the original job handoff. Source erasure removes the full
intent payload and fences dispatch; the existing minimal operation control can
retain its opaque original restriction without permitting payload replay.

Five additional producer tests are authored: both-backend actual claimed-work
admission and sticky replay; both-backend changed intent/epoch refusal with no
new source receipt, including a fresh epoch that cannot repair an old row image;
and missing-field/default/public-receipt privacy. They exercise the storage
admission boundary, not provider or complete queue-origin qualification. No
Cargo or Rust tests were executed in this worktree.

Migration 041 defines the separate private `work_producer_origin` table with
canonical epoch and actual postimage erasure guards. Queue ownership stays with
recovery. A canonical insertion of a ReprocessJob invokes recovery's original
intent initializer inside the same transaction, before outbox and receipt. This
captures its original restriction even if a crash delays creation of its queue
handoff. Updates never replace that original origin; no issued attempt or fake
queue record is manufactured. Full private producer references are ordinary
erasure payload, and their absence remains unknown/refused on later dispatch.


Original producer ancestry resolves the saved full publication fence through its
exact original operation ID. The active resolver checks the same space, physical
source, revision and generation against that row and retains its original raw
payload for the mutation guard. It never scans for a matching generation or
selects by time, revision hash, or current head. Missing original history remains
unknown or refused; these reads do not confer current authorization. Both-backend
regression sources verify the old operation after supersession, reject mixed
publication identities, and preserve unknown history across missing/foreign reads.


Standing Watch mutations carry the explicit original registration's authority.
The original queued identity is resolved through the private standing repository;
actor strings and missing identity never confer local or current-grant authority.
The producer exposes the actual attenuated principal for Source/Doc authorization
and rechecks its exact captured proof in each existing mutation transaction.
Canonical source publication separately resolves the original accepted producer
ancestry after its read-only receipt fast path and checks the resulting aggregate
proof before any new effects, outbox, or receipt on both backends. The standing
repository owns that ancestry, original ceiling and revocation logic; there is no
second publication state or mutable caller-supplied proof field. Queue, intake,
manifest, descendant maintenance, and vector consumers must join their own guards
before the recurring delegation family is considered complete.
