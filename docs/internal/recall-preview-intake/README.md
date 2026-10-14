# Retained intake acceptance

Retained MCP, HTTP and trusted Rust intake use `SourcePublicationRepository` for
acceptance. `source_intake` only prepares requests and projects the original
receipt. It owns no source head, operation table or publication state.

A replacement compares the **desired** source generation, including pending
intent. Initial creation can omit `intake`. Repeating identical content,
metadata and security under the same stable external identity returns the
original operation. A differing input without `expected_generation` conflicts.
An explicitly supplied generation requires both `revision_id` and
`idempotency_key`. Observation timestamps under `metadata.observed_at` do not
change immutable intent. Source event timestamps and extraction guidance do.

For MCP `ingest_doc`, the caller's `doc.id` is the stable external identity. It
is not the returned physical document ID. Each accepted source revision gets a
separate immutable document ID. Keep that returned ID for provenance. Source
intake uses `source.external_id`; the current physical source is resolved by
`external_head`, including after an explicit erasure/recreation transition.
A URI alone never authorizes replacing another accepted input.

Create through MCP:

```json
{
  "doc": {
    "id": "doc:11111111-1111-4111-8111-111111111111",
    "title": "Household note",
    "text": "The meeting starts at 09:00.",
    "security": {"policy_id":"household","level":2,"labels":["family"],"handling":["private"]},
    "subjects": [],
    "subjects_state": "complete",
    "metadata": {
      "extraction_skill": "extraction/household",
      "extraction_purpose": "Retain explicitly stated household facts",
      "extraction_domain": "household"
    }
  },
  "intake": {"revision_id":"upstream-1","idempotency_key":"household-note-1"}
}
```

Replace through MCP using the **same external `doc.id`** and the generation
returned in `operation.source_generation`:

```json
{
  "doc": {
    "id": "doc:11111111-1111-4111-8111-111111111111",
    "title": "Household note",
    "text": "The meeting starts at 10:00.",
    "security": {"policy_id":"household","level":2,"labels":["family"],"handling":["private"]},
    "subjects": [],
    "subjects_state": "complete",
    "metadata": {
      "extraction_skill": "extraction/household",
      "extraction_purpose": "Retain explicitly stated household facts",
      "extraction_domain": "household"
    }
  },
  "intake": {"revision_id":"upstream-2","idempotency_key":"household-note-2","expected_generation":"returned-generation"}
}
```

HTTP `/v1/docs/ingest` and `/v1/sources/ingest` accept the same optional top-level
`intake` object on their retained normalized/storage-shaped forms. Each
normalized HTTP batch item and each MCP batch item can carry its own controls.
MCP success keeps the document redaction envelope in `data` and adds an
`operation` sibling. Source intake and HTTP ingest results contain `operation`.
These retained receipts expose the internal typed operation states; the new
scoped source routes own the canonical public-state projection. No receipt
contains source text, metadata or external identifiers.

Acceptance is asynchronous. `enqueued: false` or a missing `work_item_id` after
acceptance means the durable input awaits queue recovery. It does not mean
publication completed or that the caller should generate a new retry key.
The initial queue item carries the original acceptance fence in `_publication`;
each successor claims its own attempt. Rendering or one-node promotion cannot
mark the source operation complete. The finalizer checks the whole expected
set and required indexing.

`extract_doc` deliberately reprocesses existing accepted lineage with a **new**
`intake.revision_id`, `idempotency_key`, and explicit `expected_generation`.
It creates a new immutable document and goes through the source pipeline; it
does not reopen a completed manifest or publish one node inline. Ordinary
intake never restores a retracted source or recreates erased payload.

The source text limit is exactly 2,000,000 UTF-8 bytes. Full source/document
security, subjects and raw subject-completeness state survive acceptance.
Old and proposed records are authorized before mutations. Guidance remains in
`doc.metadata`; it is not moved into transport controls.

The Rust facade is an explicitly trusted in-process interface. It accepts the
same controls and full envelope, exposes the durable receipt, and admits the
whole intake under the storage recovery barrier. `quiesce_and_export` and
`quiesce_and_import` stop and drain its own worker before storage snapshot work.
A timeout does not force-complete work or start the snapshot. Restarting workers
after explicit quiescence is the host's responsibility.

Source and test definitions are authored in this checkpoint. No Cargo build,
compiler check, tests, provider probes or deployment have been executed.

Convenience forms without an input document ID derive a stable create identity
from `intake.idempotency_key`, or from the request when no key is supplied.
Use distinct keys for independent equal-content creates. To replace an existing
convenience-created source, use its returned source identity in the source
update form, or use `extract_doc` for deliberate reprocessing. Full `ingest_doc`
continues to accept a stable caller-selected `doc.id`. Batch results retain
`succeeded` and add `operations` in the same order.

Source-backed delete means source retraction: it invalidates eligible support
through the source lifecycle authority. MCP `delete_source` and `delete_doc`
accept, for example:

```json
{
  "id": "source:22222222-2222-4222-8222-222222222222",
  "intake": {"expected_generation":"accepted-generation","idempotency_key":"retract-note-1"}
}
```

Use a document ID for `delete_doc`; the operation retracts its owning source.
HTTP accepts the same controls as query parameters, for example
`DELETE /v1/sources/source:22222222-2222-4222-8222-222222222222?expected_generation=accepted-generation&idempotency_key=retract-note-1`.
The source-backed HTTP deletion response is `202` with `{ "operation": ... }`.
Retries keep the original operation. Deleting a source-less manual document
uses the canonical manual lifecycle transaction and does not invent a source.

Restore is deliberate new intake of the retained source input. Erased payload
cannot be restored this way. MCP `restore_source` accepts:

```json
{
  "id": "source:22222222-2222-4222-8222-222222222222",
  "intake": {"revision_id":"restored-3","expected_generation":"retracted-generation","idempotency_key":"restore-note-3"}
}
```

`restore_doc` accepts the same control with the old document ID and returns a
new immutable document. HTTP restore uses `POST /v1/sources/{id}/restore` or
`POST /v1/docs/{id}/restore` with all three controls in the query string. A
source-less manual document instead uses guarded canonical restoration.

Media assets are part of the immutable accepted input. Their recoverable bytes
are retained privately under `Source.metadata._manual_doc._intake_assets`;
operation receipts contain none of those bytes. Doc and attachment records are
materialized together under the original active head fence, using distinct
physical-source/revision identities. A rejected replacement creates no media
rows. Recovery reuses the accepted snapshot rather than refetching mutable
external input. Source watches capture both generation and authorized source
version before fetching; a late response cannot claim a newer source head.

Audit inverse preparation preserves the audited before/after version check and
requires authorization of the current and proposed source/document envelopes.
Source create/restore inverses retract; update/delete inverses accept a new
revision (delete explicitly reactivates). Source-less inverses stay on the
canonical manual path. The lifecycle owner integrates the public audit adapter.

Source-backed document admission requires `read:doc:metadata`, `write:doc`,
`read:source`, and `write:source`; the retained `read:doc` alias is accepted.
HomeCore grants must include these explicitly. A document write scope does not
implicitly grant authority over arbitrary sources. Read-only/profile checks
remain at the transport boundary. Durable admission can succeed while workers
or providers are unavailable; their readiness governs completion.

When source and document inputs carry different restrictions, intake preserves
both through a conservative envelope join; conflicting explicit policy domains
are rejected. Generated empty envelopes inherit the supplied envelope. The
source's metadata is retained, and document extraction guidance stays in the
manual document template. Source-less document updates use the canonical
manual mutation path and retain their identity.

`restamp_attribution` remains a synchronous metadata correction. It prepares
all affected document/entity changes before writing and commits them with the
observed canonical epoch, exact prior records, source-support fences, audit,
and semantic outbox. It does not rewrite immutable source input or fabricate a
source attempt. Its evidence reads participate in authorization and the same
mutation read set. Oversized evidence scans fail before any change.

Document-backed correction, restore, reprocessing and audit inverse bind the exact authorized existing document into source acceptance. A concurrent change to that document rejects acceptance without advancing the desired head or persisting the proposed revision. Source-only requests retain the source snapshot guard.

The same observed document guard applies to source-backed document deletion and audit inverses that retract a source. The guard is checked within the source lifecycle transaction before journal reservation or support mutation.

Extension merge effects preserve native database field types. Their exact before/after images join A’s canonical transaction and causal inverse ledger; no helper opens a second write transaction. Inverse processing recursively preserves independently edited object fields and refuses ambiguous concurrent array edits. Declared extension tables must have A’s epoch/erasure guards installed before the merge captures its read epoch.


Explicit identity equivalence
-----------------------------

Retained HTTP `POST /v1/identity/merge` and MCP `merge_entities` prepare one complete canonical bundle before policy preflight. The bundle includes the chosen winner, input redirects, identity keys, relationships, attributes, assertions, document/event subjects, candidate references, ID mappings, declared extension references, audit snapshots, an outbox event, and the result receipt. Existing evidence and source payloads are not combined or rewritten into new source authority. The winner's derived fields remain causally dependent on the complete observed source conjunction. Erasure or source replacement reverses those decisions while retaining an existing independent baseline and later independent object fields.

HTTP example (Keep A preserves A's canonical identity):

```json
{
  "entity_a": "entity:550e8400-e29b-41d4-a716-446655440000",
  "entity_b": "entity:550e8400-e29b-41d4-a716-446655440001",
  "strategy": "keep_a"
}
```

The HTTP strategies remain `keep_a`, `keep_b`, and `combine`. Combine creates a deterministic third identity for the exact prepared inputs and redirects both inputs. MCP retains `prefer_a`, `prefer_b`, `prefer_older`, and `prefer_newer`:

```json
{
  "entity_a": "entity:550e8400-e29b-41d4-a716-446655440000",
  "entity_b": "entity:550e8400-e29b-41d4-a716-446655440001",
  "strategy": "prefer_older"
}
```

Selection uses the same stored snapshots fenced by the commit. Names do not establish equivalence. Both selected inputs must be active identities; an already redirected input is a conflict. A concurrent change to any observed row or insertion into a scanned predicate rejects the whole bundle. No winner is persisted before reference authorization. The bounded reference scan permits at most 100,000 rows per core table and fails before writing if exceeded.

Policy checks cover every observed and proposed effect. Entity, key, candidate and mapping changes require entity read/write scopes; rel/attribute changes require relationship read/write scopes; assertion changes require assertion scopes; source-backed input dependencies require source scopes; document subjects require `read:doc:metadata` and `write:doc`; event subjects require event scopes. Ordinary runtime read-only and tool-profile guards continue to apply. The full old/new security labels, handling restrictions and subject sets are preserved and authorized. The selected entities must share a representable policy domain.

Consumer extension references are selected only by registered manifests, such as:

```json
{
  "name": "sampleapp",
  "refs": [
    {"table": "sampleapp_note", "field_path": "owner", "cardinality": "one", "on_merge": "rewrite"},
    {"table": "sampleapp_note", "field_path": "nested.related[]", "cardinality": "many", "on_merge": "rewrite"}
  ]
}
```

Affected extension rows must contain an explicit `space_id`, complete `security` object, `subjects` array and `subjects_state`; absent envelopes are rejected rather than treated as public. Extension reference changes require entity read/write scope plus old/new row ABAC. Native Things, datetimes and other untouched database values remain native. Embedded opaque row identities retain their existing key representation. A installs epoch and erasure guards for declared tables before preparation. Database administrators must retain those guards. Extension before/after images join the same canonical receipt and source inverse ledger; concurrent ambiguous array or field-type changes fail closed during inverse processing.

Trusted Rust callers may use `elephant_identity::prepare_entity_merge` with `MergeSelection`, authorize `PreparedEntityMerge::authorizations()`, and then call `commit()`. The resulting `EntityMergeOutcome.survivor.base.id` is the actual committed canonical identity. The retained trusted `apply_entity_merge_rewrites` entry point prepares and commits the entire supplied group through the same boundary. These are derived maintenance operations and require no fabricated source revision or attempt.

Source responses omit the reserved `metadata._manual_doc` recovery template even
when raw source access is granted. Stored immutable revisions keep the template
and media inventory for recovery. Public metadata and full security/subjects are
preserved; URI/path additionally require `read:source:raw`. Document text remains
behind document raw authorization. HomeCore guidance belongs in `doc.metadata`.

The Rust facade's repository factories and `storage` re-export are trusted low-level
storage APIs, not source-submission APIs. Callers accepting source content must use
typed acceptance with explicit revision/idempotency/generation controls; convenience
`Elephant::ingest_text` uses that boundary. Raw access does not waive storage epoch,
space, tombstone or erasure invariants.

Maintenance sweeps call `prepare_entity_merge_on_plan` with the original
`CanonicalPlan`, then consume `PreparedEntityMerge::into_prepared_plan` and use the
planner's guarded `adopt_plan`. The returned plan retains earlier typed/native
effects and before images, includes merge audit/outbox effects, and reads pending
creations, redirects and declared extension references. It never commits or
refreshes the captured epoch. The outer sweep installs extension epoch guards
before capture and commits once with its original attempt. Standalone `commit()`
rejects a merge prepared on a caller's plan.

The retained `POST /api/v1/spaces/{space}/documents` now uses the same source
admission as `/v1/docs/ingest`. Example request (normal full Doc fields retained):

```json
{
  "doc": {"id":"doc:550e8400-e29b-41d4-a716-446655440000", "space_id":"household",
    "created_at":"2026-10-07T00:00:00Z", "updated_at":"2026-10-07T00:00:00Z", "created_by":"homecore",
    "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
    "provenance":{}, "subjects":[], "subjects_state":"complete", "title":"Note", "text":"Updated note",
    "source":null, "media_attachments":[], "rendition_key":"input", "metadata":{"extraction_purpose":"facts"}},
  "source_type":"text/plain",
  "intake":{"revision_id":"second","idempotency_key":"note-second","expected_generation":"generation-from-prior-receipt"}
}
```

Create omits `intake` or supplies create revision/key without a generation. The
response is HTTP 202 with `doc_id`, `source_id`, nullable `revision_id` and
`work_item_id`, and `operation`. `revision_id` identifies a rendered DocRevision
and stays null until it exists; acceptance never fabricates completed rendering.
The queue receives the original returned publication fence. The route requires
all four retained document/source read/write scopes and authorizes prior and
proposed envelopes. `classification` can tighten an explicitly supplied level;
omitting it does not reset security. Source type hints and extraction guidance
remain document metadata.

`POST /api/v1/spaces/{space}/entities` retains its full Entity request and 201
Entity response. Entity creation and all supplied `identity_keys` now share one
canonical commit with propagation and audit. Both entity read/write scopes and
old/new key authorization are required. An existing key assigned to another
identity is a conflict requiring explicit merge; it is never silently stolen.
A denied dependent key leaves neither the new entity nor earlier staged keys.
Independent manual entities stay source-less and require no invented intake.

Entity type consolidation and the retained opt-in classification backfill expose
prepare-only helpers for the shared maintenance sweep. Their standalone entry
points also capture one epoch before reads and commit through the same planner.
Type decisions read pending entity/relationship changes; relationship evidence
must be visible under the entity's existing security and subject restrictions.
Classification only repairs untouched baseline envelopes and resolves every
contributing revision/document in the selected space. It joins document security
and subjects while preserving source ownership and semantic content. Missing
contributors remain unresolved. Incompatible opaque policies return a typed
validation error and abort preparation of the whole sweep. No record is
written during preparation, and a concurrent document or relationship change
rejects the whole commit. The per-table scan bound remains explicit in stats;
`capped` does not claim the remaining graph was repaired. Scheduling remains
opt-in, with the outer sweep's original attempt and one durable result receipt.

Retained intake, document lifecycle/restamp, and explicit merge mutations retain
the typed storage erasure refusal through their shared error adapters. During
a reserved source erasure, HTTP returns 503 with `error_code: "not_ready"` and
`retryable: true`; MCP preserves the typed error for the shared public mapper.
Generation conflicts remain non-retryable conflicts. Classification never
searches arbitrary error text for an erasure substring.

The retained `POST /api/v1/spaces/{space}/sources` accepts the same flattened
`SourceWriteRequest` as `/v1/sources`. Its structured contact behavior is retained:
email is normalized to lowercase, profile identity remains exact, and company
plus job title prepares a `works_at` relationship. Example initial submission:

```json
{
  "id":"source:00000000-0000-0000-0000-000000000030",
  "space_id":"household",
  "created_at":"2026-10-06T20:00:00Z",
  "updated_at":"2026-10-06T20:00:00Z",
  "created_by":"client",
  "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
  "provenance":{"sources":[],"chain":[]},
  "subjects":[],
  "subjects_state":"complete",
  "type":"manual",
  "external_id":"address-book:alice",
  "hash":"contact-input-v1",
  "metadata":{"contact":{"name":"Alice","email":"ALICE@example.invalid","company":"Example Org","job_title":"Engineer"}},
  "intake":{"revision_id":"contact-v1","idempotency_key":"contact-v1"}
}
```

The selected path space and authenticated creator are authoritative. Acceptance
returns HTTP 202 with `{ "source": <public source>, "operation": <receipt>,
"work_item_id": <ID or null> }`. The source projection always removes private
manual replay data, and raw URI/path fields still require their raw scope. The
receipt contains no source payload. A replacement uses a new `revision_id` and
`idempotency_key` plus `expected_generation` from the prior receipt. A retry of
the original semantic request returns the original operation and sealed intent;
it does not resolve a new contact or adopt the latest source head.

Before acceptance, the server authorizes full old and proposed source, entity,
identity key, and relationship envelopes. Contacts need entity read/write scopes;
a prepared relationship also needs relationship read/write scopes. A hidden
company or identity record denies the whole request before any source, person,
key, operation, or queue write. Ambiguous company names or keys naming different
people conflict. Revoked keys and deleted identities are never revived, and a
deleted `works_at` edge stays deleted. Existing independent fields survive.

The storage helper performs bounded reads and seals exact records, absence
checks, and complete selector match sets in the private accepted attachment.
Admission checks the original epoch and these certificates atomically with
source acceptance. The queue carries that acceptance's original publication
fence. Graph records remain unchanged until the whole-source finalizer appends
the accepted contact effects and outbox to its real attempt's canonical commit.
The final transaction checks the sealed records and predicates again; a changed
contact dependency fails without partial publication. It does not require an
unchanged global epoch since acceptance. Structured source facts use their
sealed contact provenance rather than fabricated document passages.

Replacement preparation captures the independent baseline after retirement of
the old source contribution. Finalization composes only with that exact
retirement prefix and preserves its original before-images. Thus old
source-only aliases or fields are not copied into the replacement, while
independent contact content remains. Private attachments and their payloads are
included in source erasure. There is no public arbitrary-plan metadata field.
JSON object key order is irrelevant to intake identity and private intent
digests; array order and every scalar value remain significant.

Before transport authorization, intake asks the publication repository to preview
the exact physical acceptance source. Retained adapters never enable erasure
recreation. If the external identity moved since the initial preparation read,
preparation conflicts instead of silently retargeting the authorized contact or
document proposal. The publication transaction still checks the original CAS.

Finalization verifies the private contact seal against the original operation,
not the mutable source projection or a later acceptance's timestamps. Its plan
must retain every original contact dependency, first before-image, effect and
contact event. Grounded enrichment can add aliases, identity names, protectee
metadata and evidence, preserve an earlier name as an alias, refine a generic
topic, and tighten authorization. It cannot redirect the contact or key, change
relationship endpoints/type/validity, discard an accepted field, reduce
confidence during promotion, or omit the sealed event. Existing metadata values
stay intact. A separate relationship checker attests the structured fact and
its original envelope; maintenance also needs the publication repository's
current published-fence and support proof for later status/confidence changes.

`ingest_file` identifies a local source by its canonical absolute UTF-8 file
locator (`file:<canonical path>`), scoped by named space and source type. Equal
basenames in different directories remain different sources. File and URL
replacement preparation resolves the publication repository's external pointer
regardless of content hash and retains the source's existing complete security,
subjects and provenance. Generated document and media envelopes inherit those
restrictions. A changed input still requires explicit revision/key/generation;
old published document identities and contents remain intact.

Directory ingestion supports optional per-file replacement controls. Keys are
exact normalized relative paths shown in its results, using forward slashes:

```json
{
  "path":"/home/luka/notes",
  "pattern":"**/*.md",
  "intake_by_path":{
    "daily/2026-10-06.md":{
      "revision_id":"notes-v2",
      "idempotency_key":"notes-v2",
      "expected_generation":"<generation from this file's prior receipt>"
    }
  }
}
```

The map allows at most 1024 entries and 1,000,000 serialized UTF-8 bytes; path
keys allow at most 4096 bytes. Absolute, parent-traversing, backslash, duplicate
separator and dot-segment keys are rejected. A key naming an undiscovered or
unselected file is rejected before any acceptance. Each successful `ingested`
entry includes `path`, immutable `doc_id`, the original `operation`, and nullable
`work_item_id`. Unmapped changed files report the explicit-CAS conflict instead
of obtaining the newest generation automatically. Repeating a mapped request
returns its original operation. Directory and document batches retain their
existing per-item semantics; a typed erasure reservation aborts with retryable
NotReady rather than being buried in an item error string. Earlier accepted
items remain durable and can be retried with their stable intent keys.

Contact publication certifies each sealed contact effect against a fresh source
retirement plan. The canonical mutation still compares its exact originally
authorized row; only the contact's captured independent retirement baseline may
justify replacing that row's source contribution. A changed or omitted required
retirement conflicts. Unrelated graph effects receive no contact exemption.

The existing source decision receipt records contact-only inverse effects and
their exact contact event IDs. Its before image is the surviving independent
baseline, while its after image includes the final permitted grounding overlay.
Erasing revision r2 therefore restores independent content without resurrecting
r1. For a source-exclusive row, a certified retirement tombstone represents no
surviving baseline and the inverse physically deletes the contact row. An
already deleted original row remains rejected; this does not authorize revival
of independent or historical tombstones. Receipt persistence and retirement
certification execute in A's same canonical publication transaction.

Replacement controls require revision/key/generation together. Lifecycle-only
retraction and erasure controls use the explicit generation and stable operation
key without inventing a replacement revision; their shared decoding preserves
that distinction. Directory control preflight applies acceptance validation to
every per-file entry before accepting any file.

Native attachment staging carries each immutable asset's typed physical ID in
its transaction binding, including payload and complete access envelope. Retry
comparison checks every expected field, including required absence of optional
fields, using the canonical native representation. It does not allocate an
unrelated asset ID or repair a conflicting existing asset during replay.

Background contact intake uses `PreparedSourceContact::validate_service_authority`
to check every original and proposed contact row, including selector dependencies,
against the saved source authorization envelope. It applies the existing policy
conjunction to security, labels, handling, subjects and completeness; it cannot
expand authority to a more restricted existing contact. This is only an envelope
check. The caller must separately prove the original accepted contact capability,
preserve the source's own restrictions, and pass its original source/attempt
preconditions into atomic acceptance. It does not construct a service principal.

Identity merge fixtures explicitly register their selected named spaces before
calling the same canonical mutation path used in production. Embedded extension
rewrites use the same built-in storage-table classification as native scans:
private canonical/source controls are not consumer extensions, while unknown
consumer tables and owned tables without reference declarations still fail.

For retained storage-shaped `ingest_source` requests with an explicit `doc.id`,
a new source without `source.external_id` defaults to that stable caller document
identity. Changing the content or generating a new Source ID cannot evade its
previous source authorization. A supplied external identity takes precedence;
an already accepted explicit Source ID retains its original external alias even
if omitted in the request. The authority repository resolves the actual source
incarnation. Normalized convenience forms keep their existing identity rules.
Replacement still requires an explicit generation, revision and operation key.

For example, this complete storage-shaped request repeats the original `doc.id`
and uses the first response's generation in an already configured `household` space:
```json
{
  "source": {
    "id":"source:22222222-2222-4222-8222-222222222222",
    "space_id":"household", "created_by":"client",
    "created_at":"2026-10-06T20:00:00Z", "updated_at":"2026-10-06T20:00:00Z",
    "provenance":{}, "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
    "subjects":[], "subjects_state":"unknown", "type":"manual", "hash":"content-digest-v2"
  },
  "doc": {
    "id":"doc:11111111-1111-4111-8111-111111111111",
    "space_id":"household", "created_by":"client",
    "created_at":"2026-10-06T20:00:00Z", "updated_at":"2026-10-06T20:00:00Z",
    "provenance":{}, "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
    "subjects":[], "subjects_state":"unknown", "title":"Note", "text":"New input",
    "rendition_key":"client-note", "metadata":{}
  },
  "ingest_mode":"graph",
  "intake":{"revision_id":"r2","idempotency_key":"replace-note-r2","expected_generation":"<first response source_generation>"}
}
```
The returned `doc_id` is the new immutable physical document ID and need not equal
the stable input identity. Queued status is pending with no published nodes until
the source worker runs. Document response text/sidecar remains subject to
`read:doc:raw`.


Retained HTTP and MCP writes authorize the actual current physical documents,
including full security, subjects and completeness, even when the caller uses a
stable logical document ID. The shared preparation reads one original source
head, its immutable manual input and any closed render manifest. It retains
both present documents and expected physical IDs not staged yet. Acceptance and
retraction pass these snapshots to the authority transaction with an exact head
guard, complete document comparisons and typed absence guards. A source-less
manual caller document is also guarded without adding a fake source reference;
a document belonging to another source is refused. Physical documents for older
accepted revisions are never overwritten by a new acceptance.

Retraction stores the exact predecessor acceptance in its existing private
operation record. Restore uses that reference to locate the original documents
and admitted contact capability, including pending input and repeated retraction.
A lifecycle generation is never treated as an accepted document generation, and
an immutable revision's first acceptance cannot substitute for a later ABA
acceptance of that revision. Missing predecessor proof refuses restoration.
Restore remains explicit and requires a new revision, operation key and current
retracted generation. Identical operation retries preserve the original receipt
and original admitted contact Some/None.

`extract_doc` resolves the immutable source and original publication from its
actual document manifest mapping, requests Graph processing and passes that
original fence into reprocessing admission. A fresh request with a stale original
fence fails, even if it supplies the latest generation. A previously admitted
contact capability is freshly prepared and every old/new/dependency row must
pass full scope and ABAC checks before acceptance. Plain input containing contact
metadata remains plain. No contact graph writes happen during preparation or
source admission; whole-source finalization publishes the expected set.

Source watches capture the full acceptance fence and source authorization before
fetching. Changed content retains that exact predecessor through admission;
contact and document preflight must fit the original service envelope without a
fabricated principal. A changed document restriction cannot be bypassed by the
source's less restrictive envelope. Replayed fetch intent retains its original
admitted contact state and queue authority.

Audit inverse adapters receive `current_documents` and an optional prepared
`contact` alongside their existing source/document snapshots. They must authorize
all these rows before calling the unchanged `accept(storage, actor)` method. The
prepared retraction/acceptance retains the same observations until mutation.

Authored regression coverage includes both backends for changed current physical
Doc, previously absent staging and source-less caller CAS; retained MCP scope and
Doc-only restriction refusals; original sealed/plain reprocessing and retries;
pending restore after repeated retraction; and actual loopback source-watch
propagation, retry and service-envelope refusal. These tests are source authored;
execution is reserved for the coordinated integration runner.

The same document guard includes the original full render manifest, or its
observed absence, read when enumerating documents. That observation is compared
inside both acceptance and retraction transactions. A manifest appearing under
the same processing generation therefore cannot introduce unobserved document
rows between authorization and mutation. No later manifest read refreshes this
observation; matching operation retries retain the authority's read-only fast path.

Ordinary caller replacements also preserve an originally admitted contact
capability with fresh full contact authorization. This uses the head captured at
initial preflight and explicit caller generation controls. New plain sources stay
plain; background producers still require their original saved fence.


The trusted Rust `Elephant::ingest_text` convenience API uses the same verified
replacement capability path. An explicitly addressed existing source retains its
admitted contact class on replacement and stable retry; arbitrary contact metadata
on a new plain source does not grant contact publication. The in-process host is
responsible for authorization and supplies the full security/subjects envelope.
This does not add a fabricated transport principal or change trusted raw repository
access. Its focused source test asserts original plain/sealed admission, public
facade replacement, queued original authority, identical retry receipt/document/work
and no premature graph publication. Rust execution remains centrally scheduled.


A manual document submission with an explicit `source` link resolves that exact
source even if the document has never been stored. The retained HTTP ingest/batch
and MCP ingest paths preserve its external identity and apply the supplied
revision/generation controls to that source. Unknown or foreign-space links fail;
erasure reservation remains a typed retryable refusal. They never silently create
a different source. Document extraction guidance stays in the manual document
template, while independent source metadata and admitted contact capability are
preserved. The source and every old/new document still pass the shared preflight
and transaction guards. The actual retained MCP regression covers both originally
plain and sealed sources, source identity, queued original capability, guidance
placement and unknown-link refusal. It is authored, not locally executed.

The extension identity E2E fixture supplies its mandatory security, subjects and
completeness envelope; the real merge continues to refuse rows lacking that
policy information. The rewrite oracle also verifies that envelope survives.

The native Attribute repository encodes absent `temporal_audit` and
`validity_window` as SurrealDB `NONE`, matching their `option<object>` schema.
Present temporal objects retain their existing complete serialization. The
native regression exercises create with absence, update with a validity window
and timestamp-refinement history, get, removal, and a physical `IS NONE` query;
security, subjects completeness, source provenance and the attribute value remain
unchanged. This repairs the concrete S4.14 runtime failure without weakening its
schema or inheritance assertions. Test source is authored; execution is central.


S5.1, S5.2, S5.7, S5.8, S5.10 and S5.17 explicitly register their named space
before public writes. The shared scenario setup is used by both scenario crates;
it does not silently register spaces in production or globally in every harness.
The file-ingest oracle resolves its triage item using the actual accepted
generation and asserts the returned work ID and original source-operation fence.
Pending-versus-published search and relationship evidence behavior remain separate
scenario-owner qualifications, not implied by successful setup. Source tests are
not run in this worktree.

Accepted document projection now assigns `accepted:<physical Doc ID>` as the
physical rendition key, in the same helper that assigns its publication-scoped
ID. The immutable source template retains its original logical rendition key.
Intake staging and worker materialization/recovery therefore share one identity
mapping and retain strict immutable document equality checks. A pure projection
regression preserves the full original source/template; B owns the actual
both-backend staging/materialization/recovery and same-revision ABA composition
regressions.

Retraction preparation reports `Conflict("authorized_document_changed")` when
its current physical Doc differs from the caller's authorized snapshot of that
same ID. It refuses before constructing inconsistent duplicate guard entries;
current/proposed authorization and the final transaction guard remain intact.
The existing two-backend stale-document acceptance/retraction cases remain
unchanged and unexecuted in this worktree.

The retained Docs API ignored fixtures now exercise asynchronous acceptance:
202 plus the actual returned immutable Doc/Source/operation/work identities,
pending Doc invisibility, then the shared production dispatcher/finalizer before
published reads, chunks and search. The original document bodies, 3072 embedding
dimensions, at-least-two-chunks assertion, vector-partition assertion, exact `k`
result count, nonempty search, delete and restore positives remain. A shared
loopback provider has persisted QueryEmbedding/DocumentEmbedding approvals only;
explicit raw Doc scope authorizes the raw read tests. The extractor produces no
graph claims; these are endpoint/publication tests, not semantic-model evidence.
Every completed receipt must retain its original Source, generation and revision.

Delete and restore use explicit source generation CAS and idempotency controls.
Delete asserts the resulting retracted head and invisible Doc. Restore accepts a
new immutable physical Doc, asserts it remains pending, then runs actual work to
completion before the restored read. Historical test function names remain stable
for the exact centrally selected ignored suite, including names mentioning old
201/204 statuses; their assertions follow the current documented 202 contracts.
No direct render/embed shortcuts, synthetic completion markers, or private-head
writes qualify these fixtures. Their runtime result depends on the shared G/B/F
pipeline-driver/finalizer repairs; no tests were run in this worktree.

### Retry when an originally admitted contact plan is unavailable

The atomic original operation's contact digest proves admission independently
of the historical prepared contact payload. Physical source erasure can remove
that payload when it contains another source's erased before-image. Its absence
never turns an originally admitted contact into a plain source.

A retained HTTP/MCP source retry still requires the exact original semantic input
and intake controls, plus current and proposed Source/Doc scopes and ABAC. After
that preflight, `PreparedSourceIntake::read_only_contact_retry` may return the
original immutable input, publication fence and operation receipt. Its private
in-memory `receipt_only` marker forces `needs_processing()` to false. An original
`accepted`, `processing` or retryable receipt keeps that status; it is never
reported completed merely because its old plan cannot resume. The response has
`work_item_id: null`; no document, contact plan or work item is recreated. A key
without the matching input and authorization cannot retrieve this result.

The trusted Rust text facade follows the same no-staging/no-enqueue rule. Audit
restore preserves transport authorization before returning such a receipt.
Background watch processing refuses the unavailable plan with
`accepted_contact_plan_unavailable`, without accepting new input or enqueuing
original work. Other terminal watch receipts also avoid queue submission.

A fresh replacement/reprocess/restore requires a new explicit intent, current
CAS and fresh authorization of every contact effect. It reads the original
admission descriptor and prepares from surviving immutable input; it never
reconstructs the unavailable old plan. Storage tests on both backends and actual
MCP/facade tests cover pending receipt retries, missing scope/current Doc ABAC,
changed-input refusal, unchanged persisted tables, and fresh replacement after a
plan-loss fault. These are source-authored tests; the independent cross-source
physical erasure and publication oracle belongs to the finalization/erasure lane.
No Rust test execution is claimed by this checkpoint.

Queued source watches capture A's `PreparedSourceWatchProducer` from the original
Watch, WorkItem, WorkRun and AttemptFence before remote fetching. The same
original guard is checked in the acceptance transaction, accepted Doc/asset
staging transaction, B's initial queue transaction, and every conclusive check
transaction (changed, unchanged, not-modified, unsupported or remote failure).
No latest claim lookup supplies authority for an old response. Source/Doc/contact
service-envelope preflight and the original desired-head CAS remain required.

`check_with_attempt` returns the internal Rust result
`SourceWatchAttemptOutcome::Observed(run)` for a persisted observation or
`ReceiptOnly(original_operation)` for A's actual receipt-only acceptance result.
Only an actual fresh acceptance may stage its Doc and call
`enqueue_accepted_source_from_attempt`; matching retries leave all stored state
unchanged regardless of the original operation's pending status. Accepted work
recovery owns any acceptance/enqueue crash gap. An unavailable original contact
plan returns the exact terminal `PipelineError::InvalidState` category
`accepted_contact_plan_unavailable`; stale original-attempt errors retain B's
lost-lease classification. Queued staging refusals are propagated, never wrapped
as successful Failed checks. The existing trusted direct `check` API keeps its
`SourceWatchRun` return contract.

Watch queue identity uses A's versioned semantic intent fingerprint, including
the original actor, URI, source, interval and complete policy/provenance envelope.
Observation counters and next-check scheduling do not allocate a new identity.
The fingerprint is retained in the original immutable WorkItem payload. A policy
or actor change creates a different work intent; same-intent reopening changes
only mutable queue fields with B's original-status guard. Unknown older payloads
cannot acquire current watch authority by looking up the current Watch. Historical
Watch accounting continues to verify the original work/run and bounded producer
ancestry; this adds no public DTO or durable source authority.

Authored validation covers both storage backends: actual claimed Watch acceptance
and staged Doc, exact original receipt retry after Doc loss without restaging,
valid original guarded check followed by stale-snapshot refusal, and cancellation
after acceptance but before Doc/check writes. Actual loopback service tests cover
plain and sealed queued watches, original receipt-only full-table invariance, and
unavailable-plan refusal through the outer queued snapshot boundary. Queue tests
preserve same-intent reopening and verify new policy/actor identities. B's separate
actual dispatcher tests cover paused fetch, cancellation/reclaim and unchanged
Source head; no tests have been executed in this worktree.

### Independent original contact contribution during erasure

`PreparedSourceContact::independent_contribution_rows` is a pure internal
projection of the exact sealed original source. It does not query selectors or
write records. Every returned physical ID comes from an admitted effect. The
positive baseline contains only that input's own name, normalized email/profile
keys, company and works-at assertion, with its source envelope and provenance.
It contains no inherited aliases, summary, handles, risk/trust, arbitrary metadata,
source-type annotation, unrelated identity keys, or another source's evidence.
A deleted relationship has no admitted effect and cannot be recreated here.

These minimal rows are positive contribution baselines, not whole-row replacement
instructions or deletion masks. The integrity and erasure owners compose them
with independently supported current fields before reversing source-exclusive
creation, and guard the original admission/completion/lineage controls in the
same mutation. A source's explicitly declared predecessor provenance remains
conjunctive and must disqualify independent survival when that predecessor is
erased. A contact plan with an invalid digest or changed original source is
refused. The projector does not manufacture admission for a plain metadata source.

Source tests cover inherited sensitive fields on both storage backends, exact
selected IDs, changed-source/seal refusal, retained declared dependencies,
coalesced person/company identity, and existing relationship tombstones. They
are not an erasure qualification result; the unchanged full erasure/finalization
oracle exercises the joined composition and physical deletion paths separately.

The independent contact projector refuses inherited facts that the original
contact input cannot independently express: a selected person with another
entity type, a distinct company with a type other than `org`, non-default key
confidence, and a relationship with bounded validity or a status/confidence
other than the standalone contact defaults. Each refusal is a typed storage
conflict with a `contact_contribution_*_unowned` category. Erasure remains
pending for that unsupported ownership case; it does not silently retype,
retime, strengthen, or retain an entire inherited afterimage. Ordinary and
coalesced contact contributions retain their existing projection contract.
Both storage backends have authored cases preparing against actual persisted
non-default facts and verifying refusal without changing those facts. These
are source tests, not an executed qualification claim.

The retained MCP watch-management handlers now authorize Source read and write
before any management mutation. Configure authorizes the complete original Watch
before changing its envelope, then the complete proposed Watch before create or
update. Check authorizes Watch read and write before enqueue. Denied response
read access therefore cannot leave a persisted watch or queued work behind.
Three authored active-handler tests cover independently missing Source scopes at
create/update/enqueue, old Watch restrictions that must not be lowered, cross-space
refusal, and successful full-envelope preservation. They use actual accepted
Source input, normal non-skipped ABAC and full persisted-table comparisons.

The initial ordering-only checkpoint did not yet provide atomic registration.
The standing-delegation extension below adds that transaction and Doc preflight.
`Maintenance` remains a tool profile, never a permission grant. No Rust tests
were executed for these checkpoints in this worktree.

When a fetched hash matches the immutable desired Source but the Watch still
remembers older bytes, the watch now records a genuine `HashUnchanged` check.
This covers a crash after acceptance and before advancing the Watch. It does not
accept another generation, recover a receipt by hash, stage a Doc or enqueue
source work. The original pending operation remains owned by durable recovery.
The observation carries the full live Source, original Watch and exact raw
source-head payload captured before fetching; all are checked in the same check
transaction, along with the original attempt for queued execution. This extra
read set is restricted to the already-accepted-bytes branch because a genuinely
changed fetch intentionally replaces Source/head before its check.

Authored regressions cover an actual accepted-and-enqueued replacement with an
unadvanced Watch, cancellation, a new real claim and loopback fetch: the result
is a real unchanged observation while every other persisted table remains
identical, including the original pending receipt and child queue. Both storage
backends additionally exercise a successful guarded check, independent Source
reclassification, and a real successor acceptance between capture and commit;
stale cases leave the Watch/checks and current Source/head/receipt/Docs unchanged.
No Cargo or Rust tests were run in this worktree for this checkpoint.

Standing-watch registration extension (source under integration): signed MCP
configure/check requires retained verified caller context, an actual current
named-space grant and both Source and Doc read/write authority. Configure checks
the old and proposed Watch, the observed Source and every current desired Doc.
Its transaction checks those exact records, head, document absences and manifest
plus the grant/configuration and previous delegation observations, then persists
the new private delegation and Watch together. A missing historical delegation
requires authorized reconfiguration; it is never inferred from an actor string.
Pausing remains an explicit configuration operation; paused watches cannot
dispatch. JWT expiry limits ingress use, while a deliberately recurring
delegation remains bounded by current grant/configuration and its original scope,
clearance, subject and purpose ceiling. No bearer token is retained.

Queued service preflight checks the current Source/Docs before fetching, and
authorizes old/proposed Source/Doc/contact envelopes again before acceptance.
Every real queued observation reauthorizes the current Source/Doc read set under
the original producer principal and checks it in the same observation write.
The accepted-document recovery writer resolves the original operation's Watch
delegation even without a live Watch attempt, checks Source/Doc/asset envelopes,
and fences the original authority in the staging transaction. Other descendant
write hooks belong to their original owners; this intake checkpoint alone does
not claim complete standing-authority closure.

The authored registration tests cover actual signed MCP admission, all four
required scopes, missing verified context, stricter current Doc, current grant
revocation, and both-backend atomic races against Source, Doc, Watch, head and
configuration. Explicit trusted-local fixture registration uses the same stored
delegation transaction. Actual accepted-source recovery tests remove only the
staged projection, then prove successful recovery under the original delegation
and refusal after explicit reconfiguration, preserving the original head and
receipt. These are source-authored tests, not executed validation results.

The native already-accepted-bytes observation guard changes a private nonce on
its originally observed, present `source_head` row and invokes the existing
private reference guard to advance the shared Source-writer revision. Source
write events use that same revision. Semantic head payload and every Source
field remain unchanged; the existing private-key and erasure-reservation checks
remain active. A same-value payload/timestamp UPDATE was insufficient because
pinned SurrealDB optimizes unchanged rows away; that earlier conflict claim is
withdrawn. No new Source field or missing control row is invented.

Native source-authored controls assert an actual nonce/revision change, unchanged
semantic Source/head, and simultaneous head-only and Source-only transaction
conflicts with automatic retries disabled. Existing both-backend stale-input and
no-G3 service regressions remain. Execution is pending central validation; the
controlled writers are race fault injection, not qualification evidence for
successful publication or erasure.


The standing registration and accepted-document staging transactions also guard
the full current live Source using the existing private Source-writer revision.
Staging authorizes both current and projected Docs under every original standing
principal and refuses drift between the live Source envelope and immutable input.
Shared A document guards bind the exact original head and present/absent Doc
identities; their native physical-key conflict guards require migrations 042/043
and E's matching actual writer events. No same-value UPDATE is relied on.
The recovery regression also covers stricter current Source policy without a new
acceptance: no missing Doc may be recreated under the old envelope.

Retained MCP inputs are unchanged. The caller must present a verified named-space
service identity with a current active grant and `read:source`, `write:source`,
`read:doc`, `write:doc`, plus sufficient clearances/subjects/purpose. Existing
runtime write and Maintenance-profile gates remain in force. For example:

```json
{"source_id":"source:550e8400-e29b-41d4-a716-446655440000","interval_seconds":3600,"enabled":true}
```

Pass that input to `configure_source_watch`. To pause, call the same tool with
`enabled:false`; explicit authorized reconfiguration rotates the private
registration. Paused checks refuse before enqueue. After enabling, call
`check_source_watch` with:

```json
{"source_id":"source:550e8400-e29b-41d4-a716-446655440000"}
```

Immediate ingress proves both its own current authority and the stored owner's
current attenuated authority. It does not silently rotate the owner's delegation.
`get_source_watch` remains the read-only observation surface.

Trusted Rust hosts retain synchronous convenience through an explicit typed local
admission. Given an actual bounded `elephant_policy::Principal`, stored Watch and
StorageClient:

```rust
let admission = elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(&principal)?;
let service = elephant_pipeline::source_watch::SourceWatchService::with_trusted_local_admission(
    storage.clone(), admission,
)?;
let observed = service.check(watch, "host-audit-actor").await?;
```

The principal is the host's deliberate authority ceiling; the actor string grants
nothing. Existing local registrations require the exact original ceiling. An
initial absent registration is guarded in the registration transaction, so a
concurrent signed registration cannot be adopted. Signed recurring watches must
use their registered service path and refuse local adoption, including after
revocation. `new`/`with_fetch_policy` create worker services and do not grant direct
local authority. The explicit local fetch-policy constructor preserves controlled
loopback use while requiring a nonzero byte limit and timeout.

The convenience method registers only when legitimately absent, uses B's atomic
queue method, claims the actual original Work, creates a real WorkRun with its
lease fence and reservation, and invokes the same `check_with_attempt`. It wraps
execution in the real provider usage observer, records/settles the actual run,
and finishes work under the original lease. Ordinary HTTP observations have no
model call receipt, so usage remains `not_started` with absent measured tokens.
Timeout/error outcomes are retained, and unexpected persistence failures remain
for durable recovery. No stack limit, attempt, completion or measured usage is
fabricated. A receipt-only source replay returns a precise no-new-observation
error from this convenience API; source status remains available separately.

Additional authored controls cover authorized pause/resume, initial local
registration racing with real signed MCP registration, revoked signed local
refusal, wrong/missing constructor authority with zero loopback requests, and
real local queued WorkRun/accounting on changed text and sealed contacts. Source
validation here consists of direct rustfmt, diff checks and static API review;
all Rust/native runtime results remain pending central integration validation.


Source-processing consumers share the pure
`source_watch_registration::authorize_standing_source_input` mapping:
Source requires `read:source`/`write:source`; Doc, DocRevision, DocNode and
WorkArtifact require `read:doc`/`write:doc`. All five preserve the complete
security and subject envelope, including unknown-subject restrictions. There is
no new artifact capability and no caller-supplied replacement scope. B's aggregate
retains actual original rows after checking every applicable principal; A/F can
check proposed images through the same pure policy without inventing an original
row. Unsupported canonical record kinds refuse this input helper and retain their
own graph-operation policy. The pure helper does not grant source/queue authority.


When composing the independent native staging repair `8fd062e`, retain its
actual present-head nonce alongside the standing family's stronger original
Doc/head physical-key guards. Both leave semantic head fields unchanged. The
native staging regression from that isolated checkpoint remains applicable to
the composed writer; its new-input and stale-history assertions must be retained.


Managed MediaAsset values do not implement `HasSecurity`. Their staging policy
uses the typed pure `authorize_standing_media_asset` helper, which projects every
base security/subject field and requires the existing document read/write scopes.
It introduces no asset capability or execution authority. The same helper is
available to owners of actual media reads. Before-manifest multimodal rendering
loads managed asset payloads through `media::load_media_bytes`; a dispatch-input
proof that covers only Source/Doc cannot claim to capture that separate live
asset read without an additional original asset guard. The F/M/G owner review of
that active path remains a standing-family qualification dependency.

## Native observation fixture result shape

The native Watch concurrency fixture decodes the original and committed scalar
writer revisions through the pinned SDK's supported `Option<i64>` query result,
then requires each to exist. The original real increment assertion is unchanged.
This is a source correction for the central Runtime12 compile diagnostic; intake
did not run Cargo or claim the concurrency oracle executed.

## Standing staging trait visibility

The shared intake module imports the existing `Repository` trait for its current
Source read in the accepted-document staging guard. This addresses the central
Runtime14 compile diagnostic without changing the read, original authorization
snapshot, or same-transaction fences. Intake ran only formatting/static checks;
Cargo validation remains with integration.
