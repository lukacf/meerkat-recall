# HomeCore operational contract

This packet accompanies [the canonical wire examples](examples.json) and [route contract](README.md). It describes source interfaces, not a deployed or qualified service. Integrated builds, real provider qualification, crash recovery and clean artifact execution have separate receipts. No production cutover or paid probe is implied.

Recall is an optional, independently deployed service. Keep HomeCore's named spaces: a service token can authorize several spaces, but each request selects exactly one. There is no default-space substitution. Existing embedded and SurrealDB backends, standalone mode, wiki and guarded lifecycle interfaces remain supported surfaces.

## Authentication and provisioning

HTTP and MCP send `Authorization: Bearer <token>`. Path-selected requests use the space in the path. MCP, readiness and operation polling select a space with `X-Space-Id: research`; if a path and header are both present they must match exactly. The signed `spaces` ceiling is intersected with the current local grant for that service subject on every dispatch. An expired token, removed space ceiling, inactive grant or missing operation scope denies access. Subject identity alone grants no space. Legacy single-space tokens retain their explicit `space_id` behavior; a present empty `spaces` list grants nothing. A signed `space_id` may also supply selection when it is inside a present `spaces` ceiling; the server never selects the first array entry. See [C1-C22 follow-up](homecore-followup.md) for exact compatibility and remaining gaps.

The local CLI provisions secrets and tokens without a running API. These are command templates; they have not been executed as part of this packet:

```sh
meerkat-recall service-token bootstrap --secret-file /run/recall/signing.key
meerkat-recall service-token issue --secret-file /run/recall/signing.key \
  --subject recall-operator --global-admin --expires-in 900 \
  --scope manage:grant --scope manage:provider \
  --scope read:space_config --scope write:space_config \
  --scope read:status --output /run/recall/operator.token
meerkat-recall service-token issue --secret-file /run/recall/signing.key \
  --subject homecore-recall --space research --space planning --expires-in 86400 \
  --scope read:source --scope write:source --scope read:doc:raw \
  --scope read:doc:metadata --scope write:doc --scope read:status \
  --output /run/recall/homecore.token
```

Files are newly created with private permissions; commands refuse to replace existing files. Configure the service with `JWT_SECRET_FILE` pointing to the same signing key, and no simultaneous `JWT_SECRET`. The CLI limits normal token issuance to 31 days and global management issuance to one hour. Global management credentials cannot select a space for ordinary data access. Supply the matching explicit `X-Space-Id` for space creation and grant bootstrap. Matching `--issuer`/`--audience` values are necessary if the verifier is configured to require them.

Create each space before its service grant. The administrator uses `POST /v1/spaces` with the `space_create.body` in [operator-template.json](operator-template.json); supplying `processing` also requires explicit `manage:provider`. Then use:

```http
PUT /v1/spaces/research/grants/homecore-recall
Authorization: Bearer <operator-token>
Content-Type: application/json

{"active":true,"scopes":["read:source","write:source","read:doc:raw","read:doc:metadata","write:doc","read:status"],"expected_generation":null}
```

Repeat per granted space. Save the returned `generation`; updates and revocation (`active:false`) require that exact `expected_generation`. Grant revocation is checked at request/tool admission. Already admitted work may complete; the current contract does not promise commit-time cancellation of in-flight work or individual-token revocation. Grant scopes can only attenuate token scopes. Wildcards do not manufacture explicit `manage:grant` or `manage:provider` authority. Retained graph tools need their own scopes, for example `read:entity`, `read:relationship`, `read:assertion`, `read:event` and `read:predicate`; add only the tools the application uses to both ceilings.

Security clearances are separate from operation scopes. `--clearances-file` accepts an array such as `[{"policy_id":"default","max_level":0,"labels":[],"handling_allow":[]}]`. `--subject-allowlist-file` accepts entity record IDs. An omitted allowlist is unrestricted by subject; an explicit empty list is restrictive and must not be erased by client defaults. Restricted callers require complete, authorized subject lineage. Do not mark subjects complete merely to make a denial disappear.

Explicit nonempty `clearances` select their exact policies. Omitted or empty `clearances` retains the legacy fallback from `security_level` and `security_labels` for `default` and `tenant_policy_v1`; it is not a deny-all representation. The tenant compatibility clearance includes `handling:trusted_context`. New household consumers should send explicit household clearances. A shared MCP bridge must choose a separately authorized token for each child/adult/owner operation; passing an adult token plus a child label does not attenuate authority. Handling caveats are exact required memberships: a voice-context token must lack permission for `no_voice`, while a text context can be granted it. Recall does not control what a caller does after receiving data. HomeCore owner decision `9b8571` (2026-10-09 15:30:27Z) allows Voice to read agent-written Recall records like other records: do not add `no_voice` solely because the writer is an agent. Keep the writer's authorized audience and all applicable source/security lineage; explicit record handling caveats continue to use the existing membership checks.

HTTP `POST /v1/spaces` and MCP `create_space` now accept optional top-level `security` at creation, with read/write space-config clearance checked before persistence. Omission retains `default`, level 0, empty labels/handling. Ordinary updates cannot relabel it. For example, a shared household space can use `{"policy_id":"household-v1","level":0,"labels":[],"handling":[]}`, with higher per-source levels and audience labels. No active policy registry assigns meanings/ranges to these strings: the trusted issuer defines that convention and signs matching clearances. A global CLI bootstrap token intentionally carries only legacy level-0 clearance; custom creation can use an explicitly minted legacy single-space `space_id` token with matching clearance and space-config scopes. In-process HS256 signing remains supported; the CLI is not mandatory.

To rotate a service token, issue a new private output file for the same subject and spaces, switch the consumer to it, then retire the old file after its expiry. Both tokens remain valid during overlap. Current grant revocation applies to both. To revoke one credential independently, use a distinct service subject and grant; this contract has no individual JWT revocation list. Changing the signing key invalidates all tokens signed by the previous key; it is not an overlapping key-ring rotation. Keep key handling separate from source backups.

## Mapping the deployment manifest

HomeCore owns `deployment/recall.toml` and its translation. Recall does not read that file or provide a TOML-to-SpaceConfig endpoint. The exact HomeCore file was not supplied with this source packet, so the table maps deployment concepts, not invented TOML key names. [operator-template.json](operator-template.json) supplies concrete Recall request bodies to target.

| Deployment concept | Recall field or operation |
|---|---|
| Service base URL and bearer file | HomeCore transport configuration; no SpaceConfig field |
| Named space | `POST /v1/spaces` body `name`; exact path/header selection thereafter |
| Default extraction purpose and skill | Space create/update `purpose`, `skill` |
| Approved account, provider, endpoint and secret reference | `processing.connections[connection_id]` |
| Immutable model/version, dimensions and preprocessing | `processing.profiles[profile_id]` |
| Approval for one model operation | `processing.bindings[operation_name]` |
| Current vector profile | `processing.active_embedding_profile_id`; later changes require staged reindex and explicit activation |
| Classification and compartments | Source `security: {policy_id,level,labels,handling}` and caller clearances |
| Subjects and completeness | Source `subjects`, `subjects_state`; retained `doc` base fields for `ingest_doc` |
| Source/initiative linkage for search | Source `labels` map; query `filters.labels` |
| Per-document extraction instructions | Source PUT `metadata`, or retained `ingest_doc.doc.metadata` |
| Source observation and identity | PUT path external ID plus `revision`, `observed_at`, `source_url` |
| Exact evidence and provenance | Server-owned source/revision/node lineage, operation receipts and model-binding provenance |

When creation omits `security`, a space uses the existing `default` policy at level 0 with empty labels/handling. Creation can set the explicitly authorized initial envelope described above; ordinary updates cannot replace it. A source can provide its explicit complete `security` object; otherwise a new source inherits space security and replacement inherits the authorized source envelope. Reclassification still requires authorization of both old and new envelopes. Source `labels` are application metadata and confer no security authority. `processing.policy_id` identifies processor approval policy; it is not a substitute for the source's security envelope.

A remote connection contains a **reference**, not a secret: current `EnvironmentCredentials` resolves `credential_ref` as an exact environment-variable name made of ASCII letters, digits and underscores. No `env:` prefix or conventional-key fallback is implied. Missing credentials block dispatch. A connection's account and endpoint are immutable; credential rotation changes its reference/generation and increments the configuration generation, without changing vector identity. Never put secrets in URLs or metadata.

New configurations start at generation 1. Fetch current configuration before replacement and preserve storage-owned `index_generation` and pending reindex state; actual changes advance the generation. Model profiles are immutable: choose a new profile ID for a semantic model/version/dimension/preprocessing change. `text-v1` embedding preprocessing preserves exact text. For an empty space the initial approved profile can be active. For a populated space, initial profile activation and later changes use `/processing/reindex/start`, wait for complete current node/entity coverage, then `/activate` with the observed configuration generation. The old profile remains queryable during a normal replacement reindex; completed staging does not activate itself. Revoked/missing bindings cannot silently fall back to another model or space.

Public source PUT selects Graph processing. Exact graph publication requires compatible approved `query_embedding` and `document_embedding`, plus `extraction` and independent `support_verification` when semantic candidates are produced. The operator template includes that verifier binding. Current default triage is deterministic, so its model binding is optional; default text rendering does not generate summaries. Selected media, summary, wiki, truth-maintenance and disambiguation workflows require their own explicit approvals when invoked. Space status reports optional triage availability but its base graph `providers_ready` gate uses query/document embedding, extraction and support verification. Availability checks local credentials/assets, not a paid provider probe or a guarantee of endpoint health.

Approvals are separate for `query_embedding`, `document_embedding`, `reindex_embedding`, `extraction`, `triage`, `support_verification`, `summary`, `promotion_disambiguation`, `truth_maintenance`, `wiki_writer`, `wiki_verifier`, `wiki_planner`, `media_render` and `identity_disambiguation`. Only approve intended operations. Admission of a source is durable intent, not proof that every later workflow has its required approval.

FastEmbed assets must be provisioned and pinned independently of this packet. The packaging continuation binds a `file:///absolute/assets-root` connection to `model_version: sha256:<manifest-byte-digest>` and verifies the manifest's model identity and asset hashes; missing assets are unavailable, never a license to download mutable `main` or substitute a model. Artifact availability and real model execution remain unqualified until their separate checks pass.

Per-document guidance uses this agreed precedence: explicit `doc.metadata` → source metadata → space defaults; domain inference and environment/type defaults follow explicit guidance. The grounding source checkpoint implements that order. Preferred keys are `extraction_skill`, `extraction_purpose`, `extraction_domain`. Retained aliases include `extraction_skill_id`, `skill`, `skill_id`, `purpose`, `domain`, `genre`, `content_domain`. This content guidance grants neither provider approval nor data access. Model provenance records the space/config generation/policy, operation, binding ID and profile ID; no credential reference, endpoint or secret belongs in returned provenance.

## Readiness and durable admission

`GET /v1/health` is unauthenticated process liveness. A HomeCore gateway checking only that endpoint cannot infer readiness. `GET /v1/readiness` requires bearer/current-grant authorization (use `read:status`) and explicit space selection for a multi-space service token. Its required fields include `ready`, `configured`, `storage_ready`, `worker_ready`, `semantic_ingest_ready`, `recovery_ready`, `draining`, and `runtime_mode`; status is 200 or 503, with a nested typed `error` when not ready. `GET /v1/spaces/{space}/status` adds the selected space's provider/capability status without secret values. `approved_operations` describes authorization, while `available_operations` additionally checks local credential references or pinned FastEmbed assets. `providers_ready` requires those checked operations to be locally available; these checks make no network request and do not prove remote account/model health.

A running API-only service can be ready for its configured role while `worker_ready` is false. Managed workers make their required readiness checks part of overall readiness. Source PUT can durably accept work while model execution is unavailable; its operation remains pending or reports a truthful blocked/failure state. Neither health, readiness, status, a queue acknowledgement nor HTTP 202 proves that an exact source revision is searchable. Poll the operation and use revision-constrained search coverage.

Managed serve binds the listener before starting self-dialing workers. It verifies schema; apply migrations with the separate `meerkat-recall migrate` path. Managed embedded service construction always supplies its configured persistence destination, including first startup. Its private checkpoint/WAL recovers acknowledged mutations before admission. API/library clients explicitly constructed in memory remain possible. Shutdown and named snapshot operations drain workers and use the storage barrier; failed drain or failed recovery must not report successful persistence.

## IDs, bytes, labels and retry behavior

Space IDs are 1..256 UTF-8 bytes, external source IDs 1..1024 and revisions 1..256. They are exact, nonempty, trimmed, control-free strings. HomeCore's `^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$` subset is accepted. Avoid dot-only path IDs. Percent-encode each path component once; append lifecycle `:retract` or `:erase` after the encoded external ID. Embedded slash/colon IDs require an intermediary that preserves encoded segments. The raw `X-Space-Id` must match the decoded path; non-ASCII names require a client that sends UTF-8 header bytes. Empty, duplicated, malformed or untrimmed space headers are rejected, never replaced by a token or development default. No Unicode normalization or case folding is performed.

Source text is nonempty UTF-8 with a **2,000,000-byte** maximum after JSON decoding; the JSON body cap is 12,100,000 bytes to permit escaped text. Media types are `text/plain` and `text/markdown`. Structured TOON/JSON text can be submitted unchanged as plain text. Revisions are opaque caller-selected identifiers, not timestamps or sortable sequence numbers; choose a new revision when text, labels, security or semantic metadata changes. Evidence spans are half-open UTF-8 byte offsets into the exact supplied revision, and `quote` must match those bytes. The additive `rendered_span`, `rendered_quote_sha256` and `rendered_text_sha256` independently bind support in the exact immutable rendered revision; normalization may make the raw and rendered intervals different. See [render coordinates](search-render-coordinates.md) for the frozen source contract and integration limits.

Labels permit at most 128 keys, each at most 128 bytes. Values are trimmed, nonempty, control-free strings of at most 1024 bytes, or lists of 1..128 such values. The full encoded labels map is at most 16,384 bytes. Query `filters.labels` accepts up to 64 keys: all keys must match, a scalar matches membership, a list matches any requested value. Empty lists are invalid. Source metadata is at most 32,768 encoded bytes; `_`-prefixed control keys are reserved. These metadata labels are distinct from `security.labels` and provenance.

`Idempotency-Key` is optional and at most 512 bytes, trimmed and control-free. Omission derives a stable key from the exact space/external ID/revision. Identical immutable payload replay returns the original operation even if `observed_at` or `expected_generation` changes; those two fields do not affect the immutable fingerprint. They do not permit a conflicting payload under the same revision. A new mutation uses the exact current generation. On `conflict_generation`, fetch the head and reconcile; do not blindly overwrite. Reactivation after retract requires `reactivate:true`, recreation after erase `recreate_after_erasure:true`, and both require the current tombstone generation. Ordinary retries never set those flags.

## Search, heads and reconciliation

Canonical search is `POST /v1/spaces/{space}/search` or MCP `search_scoped`. Request fields are `query`, `limit` (1..50), `filters`, and `source_revision_constraints` (up to 100 external-ID → revision entries). Explicit `ranking_mode` is `lexical|semantic|hybrid`. Lexical requires no model; semantic uses actual matched indexed nodes without lexical fallback; hybrid combines signals. Omitted mode preserves the legacy default hybrid; legacy `use_vector:false` selects lexical, `true` hybrid. Supplying both selectors, or null selectors, is invalid. The semantics checkpoint owns this additive DTO and engine join.

Every response contains `results`, `coverage` and `partial`. Each result has `kind`, `text`, `score`, and nonempty `evidence` with exact external `source_id`, `revision`, `span`, `quote`, plus explicitly named internal revision IDs. Vectors are omitted. Coverage is keyed by external ID, with `desired_revision`, `published_revision`, `state`, and `requested_revision` where constrained. `complete` for the current desired revision requires its current operation to be completed, including a fresh A → B → A acceptance whose old A evidence remains readable. An explicitly requested older published revision different from desired can have complete historical coverage. An empty result set with complete coverage can legitimately mean no match. Missing/stale/withdrawn/incomplete coverage is not that success. Provider/configuration failures remain typed failures rather than empty successful search.

Head responses require `internal_source_id`, `operation_id`, `generation`, `desired_revision`, `published_revision`, `lifecycle`, `cleanup_state` and `state`. Lifecycle is `active|retracted|erased`; erasure in progress is `erased` with pending cleanup, not a fourth public lifecycle. Operation state is exactly `pending|running|complete|failed_retryable|failed_final`. `internal_source_id` is also required on PUT/lifecycle acceptance and operation polling; it is an opaque physical incarnation, never evidence's external `source_id`. Unknown source is typed 404 `not_found`; unknown selected space is typed 404 `unknown_space`. A bare route 404 is not either contract response.

A completed operation records its historical outcome. Later replacement, retraction or erasure does not turn an earlier successful receipt into a current visibility guarantee; unfinished superseded work reports `failed_final` with `error_code:"superseded"`. Use the latest head and exact revision-constrained coverage for present visibility. Retract/erase completion describes that lifecycle action rather than searchable publication.

`GET /v1/spaces/{space}/sources?limit=50&cursor=OPAQUE` returns `sources` and `next_cursor`. Each authorized row has canonical head fields, nullable `external_source_id`, and nullable `source_type` (retained adapters can use non-manual source kinds). Public external-ID head/PUT routes address manual sources; physical IDs distinguish any other source namespaces in the inventory. The limit counts authorized rows, from 1 through 100. Internal physical batches continue until an authorized lookahead is found or the inventory ends. Denied heads never produce extra empty continuation pages, and a continuation contains only the physical key of a returned authorized head. An all-denied inventory within budget returns `sources:[]` and `next_cursor:null`. There are no totals or denied-row counts. The scan budget is 1000 physical candidates and 10 seconds of elapsed time checked between reads; exceeding either returns `rate_limited` without partial results or a cursor. A large inaccessible prefix can exceed that budget, so completion under every inventory distribution is not promised. The embedded implementation scans hash-table keys but clones only bounded batches; this is not an indexed constant-time database promise.

Cursors are bound to selected space and caller subject, expire after 15 minutes, and become invalid across API process restarts. They are opaque client tokens; invalid/expired tokens return `invalid_request` and require restarting the pass. Ordering is by stable physical head key, not source observation time. Concurrent inserts/updates can require another full pass; this is not a snapshot or lossless delta feed. Exact per-source head reads remain the generation CAS authority.

After physical erasure, external ID and source type are null; the inventory does not retain or reconstruct erased strings. Store the external-to-internal mapping when admission/head first returns it. Explicit recreation allocates a new physical ID, so inventory can contain an older erased and newer active incarnation. A fresh consumer without that mapping cannot recover an erased external identity. Missing or unauthorized inventory rows are insufficient evidence of deletion. No new retained hash is presented as a privacy guarantee.

## Retained MCP interfaces

Use the MCP protocol handshake and `tools/call`; do not POST a tool's arguments directly to `/mcp`. Tool presence still depends on the selected profile: `/mcp`, `/mcp/agent`, `/mcp/maintenance`, `/mcp/full`. Bearer authentication and exact `X-Space-Id` apply on every request. Read-only enforcement applies even if a mutation name is supplied manually. Use `tools/list` to confirm the chosen profile instead of assuming every tool is exposed there.

| Tool | Argument shape retained |
|---|---|
| `ingest_doc` | `{doc: <Doc>}`; preserve `doc.metadata`, complete `security` and subject lineage |
| `search_docs` | `{query, k?, filters?: {collection?, include_deleted?}}` |
| `search_assertions` | `{query?, entity_id?, status?, filters?, limit?}` |
| `search_all` | `{query, limit?, include_deleted?, include?: [entities,docs,sources,relationships]}` |
| `search_entities` | `{query?, filters?, limit?, cursor?}` |
| `get_entity`, `get_entity_with_relations` | `{id: "entity:<uuid>"}` |
| `list_predicates` | `{status?, include_aliases?, limit?, cursor?}` |
| `search_events` (timeline search) | `{query?, limit?, cursor?, include_deleted?, include_candidates?, statuses?, entity_ids?, mode?}` |
| `get_source_head`, `get_source_operation` | `{external_source_id}` or `{operation_id}` |
| `retract_source`, `erase_source` | `{external_source_id, expected_generation, idempotency_key?}` |

There is no invented `search_timeline` alias in this packet; the retained timeline search tool is `search_events`. New scoped search has the canonical evidence/coverage envelope; retained tools keep their own result envelopes and authorize current records/lineage. The source-backed `ingest_doc` adapter needs `read:doc:metadata`, `write:doc`, `read:source`, and `write:source`; raw reads and later graph operations need their separate scopes. Identity merge preserves its selected winner/combine semantics but requires authorization for every changed dependent envelope: entity/key/ID-map, relationship/attribute, assertion, document, event and declared extension references. Ordinary entity scopes alone do not authorize a merge that rewrites protected dependent records. Legacy `ok:false` responses become protocol errors with `isError:true` and the shared machine-readable error classification.

Public errors contain `{error_code,message,retryable}`. Use the exact codes and HTTP statuses in [examples.json](examples.json); do not parse free-text messages. Old API error bodies may retain additive `code` and `details` compatibility fields. Source erasure's adoption gate is physical source-derived deletion plus bounded backup retention. Restore journal replay is separately implemented as a Recall requirement; it is not represented as an additional OB3 gate.
