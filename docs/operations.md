# Operating Meerkat Recall

Recall is an optional, independently operated knowledge service. Each request
selects one named space. Embedded storage, the compatible SurrealDB backend,
standalone deployment, graph records, wiki workflows and guarded source lifecycle
operations remain available. Installing an archive does not provision credentials,
approve model use, initialize a database or start a service.

## Install and initialize

Use the [installation guide](installation.md), retain the whole verified version
directory and configure its absolute `skills` directory as `PIPELINE_SKILLS_DIR`.
Service templates are supplied under `service/` in native archives. Create private
state, data, recovery, secrets, model asset and log directories for the service
account. Keep the recovery journal outside data and backup directories that can
be replaced during restore. Configure `migrate` and `serve` with the same backend,
namespace and database. Run the explicit `meerkat-recall migrate` command before
starting a new persistent service; `serve` verifies schema rather than silently
initializing it. See each command's `--help` for the chosen backend's settings.

Remote mode needs the corresponding patched SurrealDB server and capability
check. A matching upstream version number alone does not establish compatibility.
Its server binary is a separate artifact. The embedded managed service persists
acknowledged mutations using its configured durable storage destination.

## Credentials and space selection

The local CLI creates signing material and tokens without a running API:

```sh
meerkat-recall service-token bootstrap --secret-file /run/recall/signing.key
meerkat-recall service-token issue --secret-file /run/recall/signing.key \
  --subject recall-operator --global-admin --expires-in 900 \
  --scope manage:grant --scope manage:provider \
  --scope read:space_config --scope write:space_config --scope read:status \
  --output /run/recall/operator.token
meerkat-recall service-token issue --secret-file /run/recall/signing.key \
  --subject recall-client --space research --space planning --expires-in 86400 \
  --scope read:source --scope write:source --scope read:doc:raw \
  --scope read:doc:metadata --scope write:doc --scope read:status \
  --output /run/recall/client.token
```

These are templates, not issued credentials. New files have private permissions;
the commands refuse existing outputs. Set `JWT_SECRET_FILE` to the signing file
and do not also set `JWT_SECRET`. Match configured issuer/audience requirements.
Normal tokens have a 31-day maximum lifetime; global management tokens have a
one-hour maximum. Global management credentials cannot perform ordinary data
access by selecting a space.

HTTP and MCP use `Authorization: Bearer <token>`. Path requests select their
named space. MCP, readiness and operation polling use `X-Space-Id: research`.
If both path and header select a space, they must agree exactly. A multi-space
token does not select the first listed space or create a default. The signed
space ceiling is intersected with the current local grant on each dispatch.

Create each space using `POST /v1/spaces`, with the matching `X-Space-Id`, before
granting the service subject access. Use `PUT /v1/spaces/research/grants/recall-client`
with an explicitly authorized management token and a JSON body such as:

```json
{"active":true,"scopes":["read:source","write:source","read:doc:raw","read:doc:metadata","write:doc","read:status"],"expected_generation":null}
```

Record the returned generation. Later updates and `active:false` revocation use
that exact generation as `expected_generation`. Token scopes and grant scopes
both constrain access. Graph and wiki operations need their own explicit scopes.
Wildcard scopes do not manufacture `manage:grant` or `manage:provider` authority.
Revocation denies subsequent admissions; source-backed standing work also checks
its original persisted authority at its guarded storage boundaries. This does
not promise cancellation of every in-flight network request. There is no
individual JWT revocation list. Use distinct subjects for independently revocable
credentials. Signing-key replacement invalidates tokens under the old key.

Classification and subjects are separate from operation scopes. Explicit
`clearances` bind exact policy IDs, levels, labels and handling permissions.
Omitted or empty clearances retain the compatibility clearance fallback; they
are not a deny-all representation. An omitted subject allowlist is unrestricted
by subject, while an explicitly empty allowlist remains restrictive. Restricted
callers need complete authorized subject lineage. Never mark subjects complete
or drop handling caveats to bypass authorization. Metadata labels confer no
security authority. The issuer defines policy and audience conventions.

## Approve processing

A space configuration's `processing.connections` selects an account, provider,
endpoint and credential reference; `profiles` selects immutable model versions,
dimensions and preprocessing; `bindings` approves particular operations. A
remote `credential_ref` is an exact environment-variable name, without an `env:`
prefix. The reference is not the secret. Do not put secrets in URLs or metadata.

Source PUT uses Graph processing. Compatible approved `query_embedding` and
`document_embedding` bindings are required, together with `extraction` and
independent `support_verification` for semantic candidates. Summaries, media,
disambiguation, reindex, truth maintenance and wiki planner/writer/verifier
workflows require their respective approvals when selected. Optional operations
do not authorize themselves. Read current space configuration before replacing
it and preserve storage-owned index/reindex fields. Profiles are immutable;
changing model semantics means choosing a new profile. Populated-space profile
activation uses the staged reindex and explicit activation endpoints.

Governed FastEmbed takes a `file:///absolute/assets-root` endpoint and a
`sha256:<manifest-byte-digest>` model version. Use the bundled
`provision-fastembed` helper with independently selected ONNX and tokenizer files.
It needs Python 3.12+, copies selected files and never downloads model weights.
Retain model origin and license evidence. The runtime verifies the pinned files;
missing or changed bytes prevent use. Model names and installed caches do not
grant approval. Remote account health and successful inference are separate
from local configuration availability.

## Admit and reconcile sources

Use `PUT /v1/spaces/{space}/sources/{external_id}` with a new opaque `revision`,
nonempty UTF-8 `text`, `media_type` (`text/plain` or `text/markdown`) and the
source's authorized metadata, labels, classification and subject lineage.
Replacement uses the exact current `expected_generation`. Percent-encode path
components once. Source text is limited to 2,000,000 decoded bytes. Revisions
bind immutable text, labels and semantic/security metadata; choose a new revision
when those values change. Metadata keys prefixed by `_` are reserved.

An optional `Idempotency-Key` identifies the immutable request. Identical replay
returns its original operation. A reused revision with different immutable
payload is a conflict. On `conflict_generation`, fetch the current source head
and reconcile before submitting a replacement. Ordinary retry must not request
reactivation or recreation after erasure.

Acceptance is durable intent. Public operation states are `pending`, `running`,
`complete`, `failed_retryable` and `failed_final`. Poll the returned operation,
then read the current source head. Earlier completed operations retain their
historical outcome after replacement or withdrawal; they do not promise current
visibility. Unfinished superseded work reports `superseded`.

Search using `POST /v1/spaces/{space}/search` or MCP `search_scoped`. Bind
`source_revision_constraints` to the desired external IDs and revisions when
checking publication. Responses include `results`, `coverage` and `partial`.
Each result carries evidence with source/revision, half-open UTF-8 byte spans
and exact quotes. Rendered evidence separately binds the immutable rendered
revision. `complete` coverage and zero results can mean no match; pending,
stale, missing or withdrawn coverage does not establish that conclusion.

Explicit ranking modes are `lexical`, `semantic` and `hybrid`. Lexical needs no
model. Semantic requires actual indexed evidence and does not silently fall back
to lexical results. Filters cannot widen the caller's policy or subject ceiling.
Promoted assertions, entities, relationships and events require valid immutable
evidence and independently verified support. Interpretation without support
cannot become approved knowledge. Grounding is rechecked at publication.

`GET /v1/spaces/{space}/sources` provides authorized heads with an opaque cursor.
It is a bounded reconciliation inventory, not a snapshot or lossless change feed.
Store external-to-internal source identity mappings at admission. After physical
erasure, inventory does not reconstruct erased external identifiers.

## Readiness, shutdown and recovery

`GET /v1/health` is process liveness. Authenticated `GET /v1/readiness` with
`read:status` and an explicit selected space reports storage, worker, recovery
and semantic ingestion readiness. `GET /v1/spaces/{space}/status` adds local
provider/configuration availability. These checks do not call paid providers or
prove that a particular revision is searchable. An API-only role can be ready
while workers are unavailable. Pending operations remain pending truthfully.

Use the graceful shutdown path. Managed workers and requests drain before the
storage barrier; a failed drain or flush is not a successful persistence receipt.
Retain the store ownership file so overlapping writers cannot bypass exclusion.

Retract and erase use the current generation and explicit lifecycle routes:
`POST /v1/spaces/{space}/sources/{external_id}:retract` or `:erase`. Reactivation
requires `reactivate:true`; recreation after erasure requires
`recreate_after_erasure:true`. Both use the current tombstone generation.
Erasure completion requires physical source-derived deletion. Apply a bounded
retention policy to backups; hiding records from query results is insufficient.
Keep the independent durable erasure journal and restore barrier outside replaced
backup/data trees. Restore validates and replays that authority before reopening
admission. See the [snapshot guide](snapshot-durability.md) and
[embedded commit guide](embedded-durable-commits.md).

MCP retains the protocol handshake and `tools/call` envelope. Inspect `tools/list`
for the selected profile (`/mcp`, `/mcp/agent`, `/mcp/maintenance`, `/mcp/full`).
Retained ingest, graph, events and wiki tools keep their own argument and result
contracts. Use typed `{error_code,message,retryable}` errors rather than parsing
message text; a bare routing 404 is not a typed unknown-source response.
