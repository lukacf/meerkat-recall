# Preview public wire contract

Owner: surface. State authority: integrity (source/operation/receipt); provider/config authority: providers; restore barrier: durability. Status: consumer-agreed wire schema with source implementation checkpoints; integrated compilation and qualification remain pending. Source commits are not qualification evidence. Error, routes and input/output DTOs belong to surface; state enums and durable rows do not.

All routes require bearer authentication except liveness. A path-selected named space must be explicitly authorized by the principal; never select the first/default space. Operation lookup authorizes its persisted space and envelope before returning data. Source writes require write:source and read:source, queries require read:doc:raw, status read:status. Source head and operation reads require read:source and the independently retained policy envelope; erased payload is not retained to authorize metadata.

Space creation accepts an optional top-level `security` envelope in HTTP `POST /v1/spaces` and MCP `create_space`. The caller must have both `read:space_config` and `write:space_config` plus matching clearance for the proposed envelope before persistence. An existing protected space also requires record-read authorization before reporting a create conflict. The envelope is the space administration envelope and default for new sources that omit `security`; explicit source envelopes can use another policy when the caller is authorized. Ordinary space updates preserve the envelope and reject a replacement `security` field. Policy IDs and label/caveat names are exact values governed by signed clearances; there is no active policy-catalogue provisioning API or built-in `household-v1` schema.

PUT /v1/spaces/{space}/sources/{external_source_id}
```json
{"revision":"capture-42","text":"A source passage.","media_type":"text/plain","source_url":"https://example.test/source/42","observed_at":"2026-10-06T20:11:00Z","labels":{"scope":"planning","initiative_id":"42"},"expected_generation":null}
```
Optional `security` uses the existing complete Security envelope; optional `metadata` preserves extraction_skill/extraction_purpose/extraction_domain guidance. Omission must follow explicit space configuration, never silently broaden an existing envelope. Text limit 2,000,000 UTF-8 bytes; JSON body limit is 12,100,000 bytes, allowing worst-case JSON escaping; metadata is capped at 32,768 encoded bytes and labels at 16,384 encoded bytes. Creation expects null; replacement requires exact opaque generation. A retry with the same idempotency key returns its original operation. A fresh explicit key with the exact current generation can accept an identical immutable revision again (including A→B→A); conflicting same-revision payload or a stale generation under an unseen key is `conflict_generation` (409). See [revision reacceptance](revision-reacceptance.md). Timestamp observation is not source ordering and `observed_at` is excluded from the immutable payload fingerprint; replay keeps the first receipt. An optional `Idempotency-Key` header overrides the stable derived space/source/revision key. Generation preconditions are also excluded from the fingerprint.

202 response:
```json
{"operation_id":"source_operation:example","internal_source_id":"source:11111111-1111-4111-8111-111111111111","generation":"opaque-head-1","desired_revision":"capture-42","published_revision":null,"lifecycle":"active","cleanup_state":"not_required"}
```
GET /v1/spaces/{space}/sources/{external_source_id}/head returns the same head fields plus lifecycle/processing state as defined by integrity.
POST /v1/spaces/{space}/sources/{external_source_id}:retract
POST /v1/spaces/{space}/sources/{external_source_id}:erase
```json
{"expected_generation":"opaque-head-1"}
```
Both mutation responses require `internal_source_id`, `operation_id`, `generation`, `desired_revision`, `published_revision`, `lifecycle` and `cleanup_state`. Retract/erase acceptance fences visibility and stale work immediately; operation completion distinguishes physical cleanup. Erasure journal contains no payload and survives old-snapshot restoration. OB3 adoption gate requires physical source deletion and bounded backup retention; restore replay remains implemented per user mandate, not represented as an additional OB3 gate.

GET /v1/operations/{id} projects integrity-owned durable state as `pending|running|complete|failed_retryable|failed_final`, with separate `lifecycle` and `cleanup_state`, safe progress/blocked reason and completion receipt. Superseded unfinished work retains an explicit reason. For source publication, `complete` requires required search visibility including embeddings at completion; canonical publication alone is not sufficient. Lifecycle operations instead complete their retract/erase action. A completed operation remains a historical receipt when a later operation replaces, retracts or erases that source. Its revision and lifecycle fields describe that operation, so current visibility requires the latest head and revision-constrained search coverage. HTTP acceptance alone is not publication/completion.

POST /v1/spaces/{space}/search
```json
{"query":"source passage","limit":10,"filters":{},"source_revision_constraints":{"source-42":"capture-42"}}
```
Limit must be 1..50. Results are `{kind: "claim"|"entity"|"chunk", text, evidence: [{source_id, revision, span: {start,end}, quote, internal_doc_revision_id}], score}`. Evidence `source_id` always means external identity; internal record IDs use distinct names. UTF-8 byte spans are half-open. No vectors. Coverage is an object keyed by external source identity with `{requested_revision?, desired_revision, published_revision, state}`. States are `pending|running|complete|failed_retryable|failed_final|missing|stale|retracted|erased`. For the current desired revision, complete requires its current operation to be completed, even when an older published copy has the same revision string. An explicitly requested older published revision different from desired can have complete historical coverage. Old published evidence remains readable while current coverage is pending. `partial: true` when included coverage is incomplete; zero results with complete coverage is distinct from not-ready. No mixing revisions of one source into a purported current answer. Citations preserve complete validated support spans and full claim text within the 262,144-byte encoded response budget; oversized support returns `payload_too_large`. See the [bounded semantic discovery and evidence delta](search-evidence-delta.md) for progressive windows and explicit `rate_limited` exhaustion.

GET /v1/readiness and GET /v1/spaces/{space}/status expose truthful storage, worker, provider/capability and recovery-barrier readiness. /v1/health remains liveness. 200 when required capability checks pass; 503 not_ready otherwise; status payload remains diagnostic, never proves exact source revision completeness.

Errors are JSON objects:
```json
{"error_code":"conflict_generation","message":"Source generation changed; fetch its head before retrying.","retryable":false}
```
Codes/status: unauthorized 401; forbidden_space 403; unknown_space 404; not_ready 503; unavailable 503; capability_disabled 403; conflict_generation 409; payload_too_large 413; provider_unapproved 403; rate_limited 429. Input syntax/range failures require invalid_request 400; missing records require not_found 404 (accepted additions). Retryability describes safe retry of unchanged request, not automatic source-order reconciliation. A temporary per-space erasure reservation uses `not_ready` / 503 / `retryable:true`; the current runtime also drains shared activity during physical purge. See [erasure reservation and runtime availability](erasure-reservation.md) for the precise boundary and source-only adapter coverage.

MCP keeps existing named tools and profile boundaries, with explicit space context, read-only enforcement, the same error taxonomy, bounded results and vectors removed. ingest_doc retains doc.metadata guidance and Security. Retained tool names include `search_docs`, `search_assertions`, `search_all`, `search_entities`, `search_events`, `get_entity`, `get_entity_with_relations` and `list_predicates`; their profile and scope requirements remain in the [exact static tool inventory](mcp-tool-inventory.json).

MCP work-item claim responses include an opaque `lease_token`; completion requires that exact original token plus actual row read/write authorization. Existing clients must echo it and cannot substitute a worker name or a later claim token. See the [original work-item attempt contract](work-item-attempts.md) for required fields and precise refusal codes.

Read-only and draining runtime gates include attribution restamping, audit reversion and predicate-proposal validation because those tools mutate stored records. Queue listings, identity-candidate listing and snapshot export remain reads even though they belong to pipeline/maintenance profiles; their existing record, scope, backend and exclusive-barrier checks still apply.

Read operations still require storage readiness. Once full shutdown clears it, ordinary MCP reads and writes return `not_ready`, including in a configured read-only runtime. `capability_disabled` describes a write rejected specifically by an otherwise ready read-only runtime.

HTTP runtime status and MCP `get_runtime_status` expose stable worker-failure issue codes, not private worker error text. A stopped worker still makes worker and semantic readiness false; detailed diagnostics remain private to the runtime.

Labels map keys to a nonempty string or nonempty string list. A scalar filter matches membership; a list filter matches any requested value. Semantic label or security changes require a new immutable revision. `text/plain` accepts arbitrary UTF-8 text including structured TOON/JSON envelopes; Markdown conversion is optional.

Reactivation requires `reactivate: true` on PUT plus the exact retracted generation. Post-erasure recreation requires `recreate_after_erasure: true` and the exact erased generation. Integrity allocates a fresh physical source incarnation, so an old erasure-journal entry cannot delete its successor. Ordinary retries cannot reactivate or recreate. An existing head always returns a nonempty opaque generation.

## Service grants

A new multi-space service token carries `spaces: ["research", "planning"]`.
Path-selected routes select one of those spaces; MCP and routes without a space
path select an authorized `X-Space-Id` or the token’s explicitly signed and permitted `space_id`; they never select the first entry in `spaces`. The ceiling is intersected with the current local
service grant on every dispatch. An absent `spaces` claim retains legacy
single-space behavior; an explicit empty array grants nothing. Neither wildcard
operation scope nor a service subject string selects or grants a space.

An authorized administrator uses
`PUT /v1/spaces/{space}/grants/{subject}` with
`{"active":true,"scopes":["read:source","write:source","read:doc:raw","read:status"],"expected_generation":null}`.
The returned opaque `generation` is required to update or revoke that grant.
Revocation uses the same PUT with `active:false`; it retains the generation fence.
Grant administration requires explicit `manage:grant` in both signed and current
local scopes where a current grant applies. Wildcard scope alone cannot grant it.
`GET` on the same path reads a grant under that administration authority.
Provider approval remains separately controlled by `manage:provider`.

## Source head

GET `/v1/spaces/{space}/sources/{external_source_id}/head` always returns `internal_source_id`, the latest accepted `operation_id`, nonempty `generation`, `desired_revision`, `published_revision`, `lifecycle` and `cleanup_state`. Required `state` uses the same five public operation states. Public lifecycle is exactly `active|retracted|erased`; an internal erasure in progress is `erased` with `cleanup_state: "pending"`. There is no tombstone boolean or fourth public lifecycle. An absent source returns typed 404 `not_found`; an absent selected space returns typed 404 `unknown_space`. Both checks retain caller-space authorization. See `heads` in the JSON fixture for active, pending replacement, retracted, erasing and fully erased examples.

## Processing administration and snapshots

Space creation and update accept an optional typed `processing` object. Updating purpose, skill or legacy embedding metadata without this field preserves existing processing approvals. Any supplied processing configuration requires explicit `manage:provider` in addition to read/write:space_config; wildcard scope does not manufacture that authority. Outbound calls still recheck current approval at dispatch.

GET `/v1/spaces/{space}/processing/reindex` returns the current processing generation, active profile and pending reindex. POST the same path plus `/start` with `{target_profile_id, expected_processing_generation}` to stage an approved profile. `/activate` and `/cancel` require `{reindex_id, expected_processing_generation}`. MCP equivalents are `start_embedding_reindex`, `get_embedding_reindex`, `activate_embedding_reindex`, and `cancel_embedding_reindex`. Full durable coverage permits activation; it does not activate the profile automatically. Concurrent content changes require a new coverage pass.

POST `/v1/system/snapshot/export` accepts `{path,name?,retention_seconds?,quiesce_timeout_seconds?}` from a global operator with read:system. Data admissions pause, workers drain, and the storage barrier verifies quiescence before the export is captured. Quiesce is bounded to 1-60 seconds (default 30); retention is 1-2,592,000 seconds (default 604,800). These bounds apply to quiescence and retention, not total filesystem copying time. The response includes `success`, `backend`, `path`, `message`, and a checked `manifest`. POST `/v1/system/snapshot/restore` with `{path}` requires global write:system; `/import` remains an alias. Ordinary data access stays closed until journal replay and final persistence complete.

Global bootstrap tokens are explicit, short-lived management credentials. They can create spaces and grants using an explicitly selected `X-Space-Id`; they cannot use that selection for ordinary data routes. Normal multi-space service tokens require current local grants. Secret-file loading, token issue/rotation, and native/container commands are documented by the packaging lane.

MCP also exposes `get_source_head` (`external_source_id`), `get_source_operation` (`operation_id`), `retract_source` and `erase_source` (`external_source_id`, `expected_generation`, optional `idempotency_key`). All use the explicitly selected caller space and the same shared wire projections as HTTP. Existing `ingest_doc` and source adapters retain their intake interfaces. Metadata reads do not require raw-document scope; their original policy envelopes still apply after physical erasure. Mutation adapters pass the authorized live source version and policy envelope into the storage transaction, so reclassification between authorization and commit conflicts rather than publishing under stale authority.

Legacy MCP `ok:false` envelopes carry an additive machine-readable error code and retryability. At the HTTP MCP boundary they become protocol errors (`isError:true`) with bounded, safe public messages. New handlers return the typed error contract directly. Legacy message-prefix classification is only a compatibility path and does not grant authorization.

## Source inventory and identifiers

GET `/v1/spaces/{space}/sources?limit=50&cursor=OPAQUE` returns `sources` (authorized canonical head objects plus nullable `external_source_id` and `source_type`) and `next_cursor` (string or null). The limit is 1..100 **authorized rows**. The adapter scans physical batches internally until it finds an authorized lookahead or reaches the end. Denied rows never create extra empty continuation pages; an all-denied inventory within the scan budget returns an empty terminal page. Every returned/lookahead row requires current source and original operation policy authorization. There are no totals or denied-row counts. Cursors derive only from a returned authorized head, expire after 15 minutes, are bound to the selected space and caller, and are invalidated by API restart. A scan is bounded to 1000 physical candidates and a 10-second elapsed budget checked between reads; exhaustion returns typed `rate_limited` with no partial rows, cursor or counts. The feed includes physical incarnation history and tombstones. Stable physical-key ordering is not a snapshot or lossless delta feed; concurrent writes may require another full pass. Per-external-ID head remains the exact CAS authority.

`internal_source_id` is now required on accepted, head, lifecycle, operation and inventory responses. It identifies one physical incarnation; explicit recreation after erasure creates another. It is distinct from evidence `source_id`, which remains the external identifier. After physical erasure inventory `external_source_id` is null: consumers must retain their own mapping before erasure. A fresh consumer cannot recover erased external IDs, and a missing or unreadable inventory row is not proof of deletion.

Retained Source GET/search and write responses use the same public projection: private `_manual_doc` recovery templates and media are always omitted. `read:source:raw` separately permits URI/path fields; it does not expose private staging internals. HTTP document GET and MCP `get_doc` remain the authorized text interfaces, with `read:doc:raw` and current lineage checks; source head/operation responses remain payload-free.

Space IDs contain 1..256 UTF-8 bytes, external IDs 1..1024, and revisions 1..256. All are opaque, nonempty, trimmed and contain no control characters; Recall does not lowercase or normalize them. HomeCore's `^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$` portable subset is valid. Encode each URL path segment once (including `/`, `%`, `?`, `#`, and `:`); never concatenate an unescaped ID into a route. Lifecycle `:retract`/`:erase` is appended after the encoded ID and only the final suffix is interpreted. Reverse proxies must preserve percent-encoded path segments. Avoid dot-only `.`/`..` identifiers and reserved path characters in portable deployments because intermediaries may normalize them. `X-Space-Id` is the raw exact space, not percent-encoded.

The consolidated [HomeCore operational packet](homecore-operations.md) covers bearer/grant setup and rotation, processing configuration, source metadata, readiness, exact bounds, retained MCP shapes, and reconciliation limits. [operator-template.json](operator-template.json) is a configuration template, not approval to call the example provider.

Current-read adapters authorize each returned root with its real `HasSecurity`
view, including entity identity and relationship/assertion endpoint subjects.
Each contributing parent/source/revision retains its separate exact envelope
check. One reader and final fence verification cover the complete candidate
batch; a root's permission never substitutes for a parent's classification.

Pipeline status remains a diagnostic metadata view under `read:doc`. It checks
the selected revision's document/space identity and classification before
returning its ID. Node, assertion, entity and work counts include only authorized
contributors, and source/space extraction guidance requires each source envelope.
It does not return document text or imply that pending work is published; exact
publication coverage remains the source head and original operation response.

Classified named-space creation before the first service grant uses the explicit
[space bootstrap profile](space-bootstrap.md). Ordinary service tokens retain
current grant checks; global management and data authority remain separate.
