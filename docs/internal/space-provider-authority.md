# Space processor authority

Every remote model operation requires `SpaceConfig.processing`. An absent space,
missing operation approval, revoked binding/connection, unavailable referenced
credential, or stale pinned profile prevents dispatch. Process environment model
settings do not grant approval. The deployment resolves only the credential name
explicitly stored in the approved connection. A connection's account and endpoint
are immutable; rotation changes its credential reference/generation separately.

The public administration owner requires `write:space_config` and explicit
`manage:provider` for processor changes. Ingest/read/retract grants cannot create
or change processor approval. Ordinary config edits preserve processing settings.
Document metadata retains precedence over space defaults for extraction purpose
and skill; processor authority always comes from the named space.

`SpaceConfigRepository::resolve_space(space_id)` returns the config token,
processing settings and guidance. `authorize_model_operation(space_id, operation,
pinned_profile)` reloads the current record. `SpaceResolutionError` distinguishes
storage errors from missing/revoked/mismatched approval. Consumers must not turn
those failures into empty matches or switch to another space/provider.

`ProviderDispatchContext::embedding` returns the exact partition policy, a guarded
provider and safe provenance. The provider checks current approval before every
batch and every HTTP retry. API query embedding and background document embedding
have separate operation approvals. `govern_agent_build` installs a Meerkat client
that checks current approval before every model turn, including structured-output
repairs and agent continuation. It disables inherited model fallback and binds the
actual model, account and endpoint. Summary and identity resolution use the same
client through `model_client`. Wiki planning, writing and independent verification
are separate approved operations, as is assertion support verification.

A revocation observed before dispatch blocks that request. Requests already sent
cannot be recalled from the processor. Retries and subsequent turns recheck the
current authority. Processor-side data retention belongs to the approved account's
service contract; Recall does not claim to erase a provider's retained request.

Model provenance is `ModelBindingProvenance`: space/config generation/policy,
operation, binding ID and immutable profile ID. It contains no secret, secret
reference or endpoint. Vector/cache identity includes space, provider, account,
endpoint, profile, model/version, dimensions and preprocessing. `text-v1` means
exact input text without normalization; unsupported preprocessing is refused.
Credential rotation does not invalidate vectors. The model field in embedding
storage contains an opaque `recall:` partition identity; outbound requests use the
approved profile's actual model name. Synthetic embedding providers are refused
by governed processing configuration.

Profile changes are staged in a separate partition and become visible only at
explicit activation after complete current-content coverage. The source checkpoint
that introduces dispatch types does not yet qualify the activation path or either
packaged artifact. Tests use isolated loopback stubs and test-only credentials;
no paid-provider result is claimed.

## Staged reindex and activation

An administrator adds a new immutable profile and `reindex_embedding` approval,
then calls `start_embedding_reindex` with an observed config revision. Existing
query/document bindings continue to select the active profile. A dedicated
`reindex_space_embeddings` worker processes at most 256 nodes per invocation;
its cursor, target, count and completion state are durable in the space config.
Its target vector writes check current config/content epoch and node existence
inside the vector transaction. Progress is committed only after those writes
succeed. A crash before progress repeats the page safely.

Source, document, revision and node mutations atomically advance the storage-owned
`index_generation`. A changed epoch resets reindex coverage on the next page.
The worker requires exact immutable revision spans, never silently clamps invalid
UTF-8 boundaries or counts missing evidence as embedded. The target partition
coexists with the old one on both backends. A complete pass means **ready to
activate**, not already active.

`activate_embedding_reindex(observed_config, reindex_id)` compares the complete
pass epoch and current approvals in the same config CAS that switches the active
profile and both query/document operation bindings. Content mutation and activation
write the same config row, so one cannot silently cross the other's barrier.
A stale activation fails and requires another current-content pass. A continuously
changing space may need a brief explicit intake/indexing drain to finish a stable
pass. Cancellation retains the old profile. Credential rotation never requires
reindexing. Ordinary config replacement cannot forge progress, reset epochs,
change immutable profile identity or activate a replacement profile.

The kernel mutation-hook owner integrates the matching embedded epoch hook;
migration `033_space_processing` installs the SurrealDB events. Both hooks are
required before this staged-activation contract is qualified. Retained staging
vectors remain source-derived content and belong to source-erasure inventory.

## Usage receipts

`ProviderDispatchObserver` is an accounting adapter owned by the attempt runner.
It durably allocates a call ID before a physical request and receives measured
per-call token snapshots. Snapshots replace earlier snapshots for that call;
retries receive distinct call IDs. An observer failure before dispatch prevents
the request. A dispatched call without a usage receipt remains unknown. Planned
budgets are never substituted for measured tokens. The optional observer receives
safe binding provenance and cannot alter model/space authority.


Entity vectors use a derived `entity_profile_embedding` table keyed by entity
and the complete profile partition identity. Old and staged profile rows coexist;
staging does not mutate canonical entity metadata. Reindex scans document nodes,
then entities, with a durable phase/cursor. Activation requires both scans to
finish under one unchanged content epoch. Entity content changes advance that
epoch alongside source/document/revision/node changes. Current query approval is
checked again at vector retrieval, including calls holding an older query policy.
The compatibility `EntityVectorCache` handle stores no vectors: each read enters
current storage admission and reloads exact-profile, still-live entity vectors.
This deliberately trades a query scan for correctness across erasure and restore.
A future cache must prove both canonical epoch and recovery-incarnation identity.

Entity maintenance reads stored profile hashes and walks beyond covered pages,
so a large covered prefix cannot starve later entities. Derived rows retain source
reference IDs for physical erasure inventory; the integrity owner must include
all profile rows in its source dependency deletion and restore barrier. The rows
are not canonical source state, and erasure cannot be satisfied by hiding them.

Production wiki constructors now take explicit selected space and resolve approved
writer/verifier accounts before use. Their typed entry points reject another
space. Internal extraction MCP calls use the same `X-Space-Id` selector as the
public surface. Task-scoped credential resolver injection supports deployment
secret stores and real loopback handler tests without changing authority or
mutating process-wide environment; spawned tasks must propagate the resolver.


Each SDK transport also installs a current-space `HttpAuthorizer`. This checks
approval, concrete URL origin/path, and named credentials before every physical
HTTP request, including Gemini file registration and polling. It persists the
attempt owner's `before_dispatch` receipt, then rechecks approval after any wait
before attaching credentials. Embedding transport uses the same second check.
Usage is correlated with the model response's physical request; intermediate
file requests retain unknown usage rather than invented zero usage. Recovery
propagates `capture_dispatch_scope().run(future)` across dedicated thread stacks
so self-bound model adapters keep their receipt owner and secret resolver.


First approval of an already populated space also uses explicit reindex activation.
Create or update its processing authority with `active_embedding_profile_id: null`,
approve the target profile for `reindex_embedding`, and start the ordinary reindex
operation. `pending_reindex.from_profile_id` is null for this bootstrap case.
Query/document embedding remains unavailable until full coverage and activation;
existing vectors with unknown processor provenance are never silently relabeled.
An empty space may select its first approved active profile at registry creation.
Initial configuration cannot inject content epochs, progress or completion state.


Explicit `text-full-v2` embedding profiles preserve the complete input through
UTF-8 segmentation and byte-weighted normalized aggregation. The governed
factory passes the approved preprocessing identity to both remote and pinned
local providers. Existing `text-v1` profiles retain their separate partition and
send the whole input, refusing overflow instead of silently truncating. Switching
versions requires a new approved profile and the existing complete reindex path.

For FastEmbed, the asset manifest must name the same preprocessing version as
the profile. Relabeling the old manifest is refused; a new manifest has a new
SHA256 model version. Readiness streams model bytes and validates the verified
model/tokenizer configuration. The local sequence ceiling is the minimum of
512, the tokenizer's finite positive integral `model_max_length`, and the model
position capacity. MPNet reserves its padding position offset. The actual loaded
tokenizer must disable truncation, count special tokens, and either split every
input segment to fit (`text-full-v2`) or reject overflow (`text-v1`). Each local
inference batch uses the original dispatch observer after model initialization
and mutex waits. Neither that factory nor model loading invents a completed
provider call or a zero-usage receipt.

The B regression source checks governed HTTP suffix participation and original
UTF-8 byte coverage, separate version partitions, pinned-manifest relabeling
refusal, BERT/MPNet sequence limits and malformed tokenizer configuration. It
uses loopback responses and synthetic configuration bytes, not semantic model
quality evidence. Source completion is conditional on the M provider algorithm
and A profile-validator join; these Rust tests have not yet been executed.


The governed OpenAI factory uses `provider::approved_openai_config` from the
current resolved binding. That constructor and the read-only
`embedding_execution_recipe` inspection share one provider policy, including
preprocessing, physical request batch size and total HTTP attempts. Inspection
contains provenance and partition identity, never credentials; it is not a
reusable authorization or a measured usage receipt. Every actual request still
passes the original current-binding and attempt-observer checks.
