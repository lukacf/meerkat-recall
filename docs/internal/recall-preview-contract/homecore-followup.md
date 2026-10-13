# HomeCore C1-C22 follow-up

Source review for HomeCore package E `0bce3736`. This is an unqualified source contract, not a deployed acceptance result. It supplements [the operational packet](homecore-operations.md), [wire fixtures](examples.json), and [exact registered tool names](mcp-tool-inventory.json). Creation security and graph-readiness corrections are implemented in surface `e834a68`; inventory paging was corrected in `13ce4a6`. Runtime `tools/list` and provider probes have not been executed for this packet.

## C1. Security policy and initial space defaults

Active authorization requires the selected space, operation scope, a clearance whose `policy_id` exactly equals the record's, `record.level <= clearance.max_level`, and every record label and handling caveat in that clearance. Labels are conjunctive memberships, not audience expressions. `audience:*` is a literal label, not a wildcard. No runtime policy catalogue assigns names or ranges to these strings; the existing `PolicyRegistry` library helper is not connected to these routes.

HomeCore may use `household-v1` directly. Define its level/audience convention in the trusted issuer and send an actual source `security` object, for example:

```json
{"policy_id":"household-v1","level":2,"labels":["audience:owner"],"handling":[]}
```

The ingest caller needs matching clearance plus `read:source` and `write:source`. Metadata `labels` do not perform this check. A child token with `max_level:0` cannot read the example even if it has the operation scope. A token must include each required audience label; adding several labels to a record requires all of them, not any one audience.

HTTP `POST /v1/spaces` and MCP `create_space` now accept optional **top-level** `security` at creation. Both space-config read/write scopes and the proposed envelope's clearance are checked before persistence. This sets the administration envelope and the default for new sources that omit security. A shared household default can be `household-v1`, level 0, empty labels/handling, with more restrictive source envelopes. Existing protected space metadata is also authorized before reporting a create conflict. Ordinary space updates reject a replacement security field; this is not a relabel operation. Omission retains default policy, level 0, empty sets.

Explicit nonempty `clearances` uses exactly those clearances. **Omitted or empty `clearances` activates legacy fallback**, using `security_level` and `security_labels` for `default` and `tenant_policy_v1`; the latter also includes `handling:trusted_context`. It means neither unrestricted access nor universal deny-all. Do not use `[]` to represent a child deny-all policy. Send explicit household clearances and explicit restricted scopes/spaces.

HomeCore owner decision (2026-10-09 15:30:27Z, decision `9b8571`, relayed by the Recall coordinator): agent-written Recall records are not marked `no_voice` merely because an agent wrote them. Voice may read them like any other record, subject to the same named space, audience, subject, classification and operation authorization. The ordinary agent-write envelope therefore has no automatic voice exclusion. This does not clear a handling restriction explicitly attached to authorized source evidence or grant Voice a clearance bypass.

`no_voice` is an exact handling caveat. A voice-context token must lack that caveat in `handling_allow`; an authorized text-context token may include it. There is no automatic awareness of a voice channel, and Recall cannot prevent a recipient from speaking data it was allowed to read.

## C2. Shared bridge caller isolation

Each MCP operation verifies the Bearer on that HTTP request and applies the selected space, signed clearances, and current grant. There is no `role`, audience label, or clearance tool argument that attenuates an adult bearer. The bridge must choose the proper signed child/adult/owner token for each operation; use distinct subjects for independently revocable audiences and channel-specific authority where needed. A single adult token forwarded for all children does not preserve child isolation. Keep the signing secret and privileged token selection in trusted bridge/issuer code, outside model-controlled arguments.

Local grants attenuate **operation scopes**; they do not replace signed clearance or subject ceilings. An omitted subject allowlist is unrestricted by subject. An explicit empty subject allowlist remains restrictive, with the existing separately cleared `handling:subjectless_ok` exception; do not use that caveat to bypass missing lineage.

## C3. In-process token minting

In-process HS256 JWT minting remains supported. The CLI is an optional issuer, not a verifier requirement. Use the exact shared key from `JWT_SECRET_FILE`, the configured HMAC algorithm (HS256 by default), valid expiration, and matching issuer/audience if configured. The verified claim names are:

| Claim | JSON type and meaning |
| --- | --- |
| `sub`, `exp` | Required string subject and integer Unix expiry |
| `iat` | Integer Unix issuance time; legacy omission defaults to 0 |
| `iss`, `aud` | Optional issuer string and audience string array |
| `space_id` | Optional explicitly selected legacy/signed space string |
| `spaces` | Optional array of authorized space strings; present empty array denies every space |
| `scopes` | String array; singular `scope` is not an alias |
| `security_level`, `security_labels` | Legacy integer level and string-array labels |
| `clearances` | Array of `{policy_id,max_level,labels,handling_allow}`; the last two fields are string arrays |
| `subject_allowlist` | Optional array of entity record ID strings; `[]` is not omission |
| `purpose` | String array |

Present `spaces` requires the current active subject grant, even if there is only one space. The selected space comes from the route/header or an explicit signed `space_id`; the server never picks the first `spaces` entry. Thus `spaces:["family"]` without `space_id` needs a selector, while an explicit signed `space_id:"family"` can supply it. Legacy tokens with absent `spaces` retain their signed single-space behavior and do not receive the new live-grant attenuation. Use the new ceiling/grant form for revocable service audiences.

`read:doc` remains an alias for `read:doc:metadata`; it never grants document text. `read:doc:raw` implies metadata permission. The CLI limits service token issuance to 31 days and global administration to one hour; these are issuer safeguards, not a claim that the legacy verifier imposes the CLI's issuance policy on every externally signed token.

For custom initial space security, a trusted in-process issuer can mint a legacy single-space bootstrap token with the matching clearance and `read:space_config`/`write:space_config` (plus explicit `manage:provider` when supplying processing approvals). The CLI's global bootstrap token intentionally has no custom data clearance. Normal consumer tokens can then use `spaces` and current local grants after the space exists.

## C4. Route and tool scopes

These are operation prerequisites; record classification, subject lineage, selected space, current source support, read-only mode, and live grants also apply. Presence in `tools/list` is not permission to access a record. Scope aliases are normalized after the token/grant intersection, so use the same canonical scope spellings in both ceilings.

| Route/tool | Required scope or family |
| --- | --- |
| Public source PUT, `retract_source`, `erase_source`, HTTP lifecycle | `read:source` and `write:source` |
| Source head, operation, inventory; `get_source_head`, `get_source_operation` | `read:source` |
| Scoped HTTP search, `search_scoped` | `read:doc:raw`; claim/entity results also require their own record scopes |
| `/v1/readiness` | `read:status` (retained alternatives `read:system` or `read:space_config`) |
| `/v1/spaces/{space}/status` | `read:status` plus space-config envelope clearance |
| `search_docs`, `get_doc`, `list_doc_chunks`, `list_doc_collections`, `get_doc_pipeline_status` | `read:doc:metadata`; raw text additionally requires `read:doc:raw` |
| `get_doc_chunk` | `read:doc:raw`, with current document/revision/node authorization |
| `search_entities`, `get_entity`, `get_entity_context_bundle` | `read:entity`; any included dependent record retains its own authorization |
| `get_entity_with_relations` | `read:entity`; returned relationships require `read:relationship` |
| `list_assertions`, `get_assertion`, `search_assertions` | `read:assertion` and current lineage authorization |
| `list_predicates`, `get_predicate` | `read:predicate` |
| `search_events`, `get_event`, `list_recent_events`, `list_upcoming_events` | `read:event` (`read:timeline` retained alias) |
| `search_sources`, `get_source` | `read:source`; URI/path additionally require `read:source:raw` |
| `search_all` | Each included category uses its own read scope; it does not create blanket graph/raw access |
| `ingest_doc`, `ingest_doc_simple` | `read:doc:metadata`, `write:doc`, `read:source`, `write:source` for source-backed intake |
| `delete_doc`, source-backed restore | Document read/write plus source read/write and exact source intake controls |
| `relate_entities` and other graph writes | Corresponding write scope, endpoint/dependent reads, and source/evidence authorization; ordinary updates cannot relabel |
| Space create/update | `read:space_config`, `write:space_config`; processing changes also explicit `manage:provider` |
| Grant administration | Explicit `manage:grant`; current token and local ceiling must both explicitly grant it when grants apply |
| `get_health` | Global administrative principal with `read:system`; use scoped HTTP readiness for the HomeCore gateway |
| `get_capabilities`, `get_tool_schema` | Discovery for the authenticated profile; returned capabilities still reflect separate operation authority |

## C5. Exact MCP names and compatibility

[mcp-tool-inventory.json](mcp-tool-inventory.json) contains the exact **core dispatcher** name sets: 27 agent, 100 embedded full, 98 SurrealDB full because embedded named snapshot import/export are not offered there. The HTTP MCP layer additionally advertises `get_runtime_status`; full can add `initialize_runtime`, `get_runtime_config`, and `configure_runtime` according to global runtime authority/state. It filters core tools by current caller/grants, runtime readiness/read-only mode and backend, then sorts/deduplicates. In particular, named-space callers do not see global `get_health` or snapshots. There is no one fixed `tools/list` valid for every bearer and runtime state. This artifact includes the composition rules and is a static registry extraction, not a fabricated runtime response. Actual descriptions/input schemas come from MCP initialization followed by `tools/list`; `/mcp/full` also has `get_tool_schema` with `{"tool_name":"..."}`. Agent tools are read-only; write tools require a suitable profile as well as authority.

The following unregistered names are not claimed to have been removed from Recall. They may be HomeCore bridge abstractions; mapping requires the consumer's intended result schema.

| HomeCore name | Exact Recall status / explicit adaptation |
| --- | --- |
| `search_kb_nodes` | Not registered; `search_entities` for graph entities, or `search_scoped`/`search_docs` for source passages. Not a schema-compatible automatic rename. |
| `search_kb_assertions` | Not registered; adapt to `search_assertions` |
| `list_kb_assertions` | Not registered; adapt to `list_assertions` |
| `get_kb_node` | Not registered; adapt to `get_entity` for an entity, or `get_doc`/`get_doc_chunk` for document structure |
| `search_collection` | Not registered; `search_docs` accepts `{"query":"...","k":10,"filters":{"collection":"..."}}` |
| `list_collections` | Not registered; `list_doc_collections` is full-profile, not agent-profile |
| `ingest_document` | Not registered; adapt to retained `ingest_doc` or `ingest_doc_simple` on a write-capable profile |
| `get_timeline` / get timeline | No such aggregate name; `get_event` reads one event |
| recent timeline | `list_recent_events` |
| upcoming timeline | `list_upcoming_events` |
| `get_top_of_mind`, `search_top_of_mind`, `list_todos` | No direct registered counterparts. Keep HomeCore's adapter/domain behavior; this packet does not invent an equivalent or authorize cutting it. |
| `ingest_source_simple` | Retained, full profile |
| `search_sources` | Retained, full profile |
| `relate_entities` | Retained, full profile |
| `extract_doc`, `delete_doc`, `ingest_file`, `get_doc_pipeline_status` | Retained; full profile has all four, agent has pipeline status only |
| `get_capabilities`, `get_health` | Retained in agent and full; health still requires global administration |
| `get_tool_schema` | Retained, full profile |
| `search_timeline` | Not registered; adapt to `search_events` |

`get_entity_with_relations` exists in default/full profiles; it is not in the agent set. Consult the exact JSON list before selecting a bridge profile. File ingestion remains an exposed tool; its host-file authority and retained source intake rules still apply.

## C6. A → B → A revision reuse

The corrected source contract is implemented in integrity `6ba0bcd`, finalization `b04892f`, and search `cc04dbc`; it requires the coherent owner group to be integrated and centrally qualified. The **same old idempotency key remains historical replay**, even with a stale generation. A **fresh explicit idempotency key plus the exact current generation** can accept identical immutable A again with a new operation/generation and acceptance-bound document mapping. Changed semantic input under the same revision remains a conflict. HomeCore need not invent a new content-hash naming scheme.

[Revision reacceptance](revision-reacceptance.md) gives the complete request/result matrix, retry rule and source pins. Persist an intent key and generation before sending; retry them unchanged after an uncertain response. Recomputing them from a newer head expresses another intent. Old published A evidence stays readable, but coverage for current desired A follows its new operation and remains partial until that operation completes. This supersedes the gap described in the original `70614912` packet as a source implementation; no runtime/provider pass is implied.

## C7. Fingerprints and omitted fields

The public semantic fingerprint binds space, external ID, revision, text, media type, source URL, labels, metadata, **resolved** security, subjects and subjects state, and both reactivation/recreation flags. Observation time and expected generation are excluded. Storage additionally binds the immutable source input after removing system identity/timestamps/lifecycle fields and the observation metadata. Flag handling on the old-revision shortcut is part of the C6 repair; changing a flag or idempotency key alone must not be presented as current reactivation.

Omitted security inherits the existing source envelope, or the space envelope for a new physical source. Omitted subjects inherit the existing retained envelope, otherwise `[]`. Omitted subjects state inherits existing state, otherwise `unknown`. Explicit `subjects:[]` is a real semantic value. Omission and an explicit value have the same wire fingerprint if their resolved values are identical. Do not set `complete` simply to bypass restricted caller lineage checks.

## C8. Operation selection and encoding

A legacy single-space `space_id` token can omit `X-Space-Id`. A token with `spaces` can also omit it if it contains an explicit permitted signed `space_id`; otherwise operation polling requires the header. A space outside the signed ceiling is `403 forbidden_space`. A different but authorized selected space cannot see the operation and returns `404 not_found`, provided that selected space exists; a missing selected space is `404 unknown_space`. An inaccessible operation envelope is forbidden.

Axum decodes path captures, so a literal colon and `%3A` in an operation ID address the same decoded ID. Percent-encode an ID once as a path segment. Headers contain literal UTF-8, not percent-encoded space names; duplicate, empty, malformed UTF-8 and untrimmed space headers fail closed.

## C9 and C16. Readiness shapes and scope

`GET /v1/readiness` reports the runtime's configured role, not a named source or space's indexing completion. Authentication may require selecting a granted space, but that does not make the response space-specific. Use `/v1/spaces/{space}/status` for local configuration/binding capability and revision-constrained search for publication coverage. `/v1/health` remains unauthenticated process liveness.

[examples.json](examples.json) now includes exact 200 API-only and 503 draining readiness bodies plus field types. All readiness booleans are required. `runtime_mode` is one of `local_memory`, `shared_memory`, `read_only`, `maintenance`, never null. Healthy readiness omits `error`. A computed unready 503 adds `error:{error_code:"not_ready",message:"Requested service capabilities are not ready",retryable:true}`. Authentication/grant failure can instead return its normal top-level typed error before the readiness handler; it is not wrapped in this readiness body.

An API-only service can have `ready:true`, `worker_ready:false`, and `semantic_ingest_ready:false`. A managed worker service includes worker/semantic capability in overall readiness. Named-space status additionally reports `guidance_ready`: the same local installed-asset check used by extraction, without a provider call. Missing, ambiguous, unreadable or empty explicitly selected space/environment guidance makes it false and prevents `semantic_ingest_ready`. Per-document/source overrides are checked when dispatched. API-only durable admission can remain ready while this processor capability is blocked. No status proves a particular revision is fully searchable.

## C10. Search coverage and result types

Unconstrained coverage contains external source IDs observed while assembling authorized candidate evidence. It is neither whole-space inventory nor necessarily only final top-N sources: candidates can be observed before later ranking/filtering. Every returned evidence source is observed. Unseen, no-hit and pending sources need not appear. For known inputs use up to 100 exact revision constraints; use source inventory for reconciliation. An unknown requested ID yields missing coverage; an unauthorized known source fails the request rather than disclosing its coverage.

`partial` is true if any included coverage state is not complete. If all explicitly requested revisions are ineligible, search returns empty results with explicit coverage and `partial:true` without model dispatch. Empty unconstrained results and empty coverage can have `partial:false`; this says nothing about whole-space ingest completion. A pending replacement can leave the earlier published passage visible while coverage is partial for the desired revision. This also applies to A → B → A: when the requested revision equals the current desired revision, coverage follows the current operation even if an older published copy has the same revision string. An explicit older published revision different from the current desired revision can still have complete historical coverage.

The source-pinned [search evidence delta](search-evidence-delta.md) preserves complete validated citation spans and full claim text within the existing 262,144-byte encoded response cap. Oversized support is `payload_too_large`, not an arbitrary prefix. Semantic windows widen over filtered-out hits within bounded partition/candidate budgets; failure to establish the requested rank is `rate_limited`, not a falsely complete empty success or lexical fallback. This is implemented source pending runtime qualification.

Every scoped evidence row requires non-null `internal_doc_revision_id:string`, the exact immutable internal revision ID, separate from external `source_id` and consumer `revision`. This guarantee does not extend to legacy DTOs. `kind` is the closed enum `claim|entity|chunk`; explicit semantic ranking emits matched chunks, while lexical/hybrid may emit all three. Coverage state values currently include `complete`, `missing`, `pending`, `running`, `stale`, `failed_retryable`, `failed_final`, `retracted`, `erased`. Authorization/config/head races or provider/storage failures can return typed errors rather than partial success.

## C11. Completion bindings

Public source PUT explicitly requests **Graph**, independently of the generic triage default. Provision `query_embedding`, `document_embedding`, `extraction`, and `support_verification`. Finalization checks approved query/document bindings against the same active vector partition, so query approval is a publication gate even though the finalizer makes no query call. Extraction uses the approved generative binding; independent support verification is invoked when candidates exist. A missing/revoked required binding blocks processing and cannot establish completion. Graph execution also needs its compiled extraction feature and actual workers.

For other retained intake paths, Retrieval needs compatible query/document embeddings plus real node coverage; explicit Metadata/Skip can complete without vectors/graph. Current default triage is deterministic. Default plain-text tree construction has summaries disabled, so neither a triage model nor summary model is an unconditional text publication gate. Selected summary/media workflows require their corresponding binding. Truth maintenance, wiki, identity and reindex operations have their own separate approvals. An explicitly configured optional promotion-disambiguation binding must be valid; the dispatcher owner is narrowing its use to the applicable graph path.

Completion also requires the closed manifest's selected producers, validated artifacts, exact embedding work/attempts and whole-source finalizer. Successful queue admission or one successful model stage is insufficient. Configuration repair uses a new pinned processing plan under the same acceptance rather than relabelling old output. The operator template now includes the previously missing support-verification approval; optional triage remains shown separately.

## C12. Remote model pinning and profile authority

`processing.profiles` plus the current operation bindings/active profile are authoritative. Top-level `embedding_provider`, `embedding_model`, `embedding_dims` are retained compatibility fields and must agree with the active profile; mismatches are refused, not resolved by silently preferring one.

For a remote OpenAI connection, `model` is the exact model/deployment identifier sent to the provider. `model_version` is a required nonempty operator-recorded immutable provider revision or approved deployment version string; there is no enforced remote date/hash syntax. Changing it requires a new immutable profile ID and embedding partition/reindex. **The remote request does not send a separate `model_version` pin or prove that an upstream alias is immutable.** Merely writing `v1` does not pin mutable upstream weights. Use an actually fixed provider/deployment identity when available and record its provenance; no paid probe or remote immutability qualification is claimed here. FastEmbed separately enforces `sha256:<asset-manifest digest>` against locally verified assets.

## C13. Skill IDs

`homecore-family-kb` is valid if that skill is installed and discoverable, for example `skills/extraction/homecore-family-kb/SKILL.md`. Discovery uses `PIPELINE_SKILLS_DIR`, then local/repository skills, with aliases for canonical relative directory path, basename and dash/underscore variants. A slash is not required; a canonical path avoids alias collisions. Grounding checkpoint `d5cdfa6` changes explicit selection to fail closed: the selected doc/source/space/environment skill must resolve to one installed, readable, nonempty UTF-8 body before extraction dispatch. Missing, ambiguous, unreadable or empty explicit assets produce configuration-blocked processing, with no substitution of a lower-precedence skill and no model dispatch. Absent/inferred defaults remain optional. Named-space `guidance_ready` uses that same local check for space/environment guidance; an accepted config string alone is not readiness. Guidance grants no model/data authority. These are source contracts pending central runtime qualification.

## C14 and C19. Source inventory

Inventory requires `read:source` and each row's current plus retained original operation envelope. Limit is **1..100 authorized rows**. Retraction retains external identity; completed physical erasure makes `external_source_id` and `source_type` null while retaining the opaque physical incarnation and authorized tombstone metadata.

The earlier candidate-paging description is corrected: hidden candidates are scanned internally, and continuation is emitted only after authorized lookahead, using the last returned authorized key. An all-hidden inventory within budget returns `[]` and null, without denied counts or hidden-only pages. Scans are bounded to 1000 physical candidates and a 10-second elapsed budget checked between reads; exhaustion is `429 rate_limited` with no partial rows or cursor. A large inaccessible prefix can prevent completion within this budget. Cursors expire after 15 minutes and on API restart; concurrent writes can require another full pass. Missing/unreadable rows are never proof of deletion.

## C15. Revocation cutoff

Yes: signed authority and the current local grant are checked at each request/tool-operation admission. Already admitted work may finish after revoke/expiry; accepted durable jobs are not cancelled by that grant change. Subsequent requests must revalidate. This does not waive source/envelope/config/attempt publication fences or per-send provider authorization. Legacy tokens without `spaces` retain their older no-live-grant behavior, so use current service grants for the intended audience revocation model.

## C17 and C18. Strings and byte accounting

Control-free is Rust `char::is_control`, Unicode category **Cc**; it does not reject all format characters in Cf. Trimmed means `value.trim() == value`, using Rust Unicode whitespace. Validation does not normalize Unicode, case, or internal whitespace. Space/revision/external IDs and labels must be nonempty within their stated UTF-8 byte bounds.

Label keys are nonempty, trimmed and Cc-free, at most 128 UTF-8 bytes; at most 128 keys. Values are nonempty similarly validated strings of at most 1024 bytes, or lists of 1..128 such strings. The total 16,384-byte limit is the `serde_json` compact serialized label map: UTF-8 non-ASCII remains unescaped, quotes/backslashes/control escapes are encoded as required, and no pretty-print whitespace is counted. It is not the submitted wire whitespace or a count of Unicode characters.

## C20 and C21. Grant and processing identities

Grant `generation` and source `generation` are opaque **strings**; preserve them exactly for CAS. Processing `generation` and storage-owned `index_generation` are unsigned integers and are different authorities.

`processing.policy_id` names the explicit **processor approval policy**, captured in job/model provenance. It is independent of the security envelope's `policy_id`. HomeCore can choose a stable meaningful value such as `homecore-approved-processing-v1`; this name itself grants nothing. The explicitly administered connections, profiles and operation bindings supply the actual provider authority.

## C22. Raw text and rollback limits

Retained authorized text reads exist: HTTP `GET /v1/docs/{doc_id}` and MCP `get_doc` with `{"id":"doc:..."}`. They require document metadata access and current authorized lineage; `read:doc:raw` exposes text rather than a redacted body. Use HTTP for large documents because MCP responses are bounded to 262,144 bytes. These are document IDs, not the external source ID or internal document-revision ID.

There is **no currently confirmed public pending-acceptance raw export or bounded external-source-to-document locator** in the new source contract. The head/operation are payload-free; current document reads do not guarantee export of accepted work that has not materialized/published. Private `_manual_doc` and recovery media are stripped from public Source responses even with raw scope; they are not an export API. Retained document search can discover accessible documents but is not a lossless source inventory or an exact pending-write export.

Therefore the existing raw document path alone cannot establish HomeCore T7 lossless rollback of every accepted post-cutover write. Preserve the submitted input in the consumer ledger, or establish and validate an explicit drain/export procedure with known document locators. This remains a contract/qualification gap for pending-write export, not a claim that data can be recovered from unsupported head fields. Production remains unchanged.
