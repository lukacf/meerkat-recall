# Actual managed-worker construction recipe

The existing authorized global MCP `get_runtime_config` inspection adds
`managed_worker_recipe` while an actual managed worker instance is retained and
has not stopped. Public health and readiness fields are unchanged. This member
is an observation and is not written into `elephant_config.toml`.

The `recall-managed-worker-recipe/v1` value contains a unique construction
`instance_id`, actual `backend`, `work_queue` and `embed_worker`. Its snapshot
borrows the same `PipelineConfig` immediately before that value is moved into
`run_pipeline_with_shutdown`; inspection does not parse the environment again.
The worker owner freezes the same `work_queue.doc_tree` value for actual render
work, including explicit settings, rather than constructing another default at
work execution. A stopped or absent worker cannot supply a current recipe.
Both inspection and readiness also inspect the original thread's finished state:
an early runtime-builder error or unwinding cannot leave the recipe current just
because execution did not reach the normal stop-flag update. Private error and
panic contents remain outside the public readiness and configuration response.

The queue observation records enabled state, work and extraction concurrency,
item timeout, transient attempt/backoff bounds, allowed/disabled work kinds and
the actual complete `DocTreeConfig`. The embedding observation contains the
global worker construction template's enabled state, batch size, provider, model
and dimensions. It is **not** an assertion about a named space's governed provider
binding or physical HTTP retry policy. Those actual factory values require their
separate owner-backed inspection and current space configuration evidence.

No complete `PipelineConfig`, database password/user/address, provider credential
or signing material is serialized. The allowlisted DTO is separate from storage
and provider authority. Existing global `read:system` / `write:system` inspection
permission applies; this does not create a new data-plane route or admission right.

Qualification must retain the original actual MCP request/response bytes and
compare the same worker instance at its boundaries. Saved TOML, defaults, health
and an old recipe cannot replace that observation. The bootstrap producer captures
this authorized read inside its existing HTTP/byte/deadline bounds. This source
checkpoint requires the worker owner's frozen tree configuration seam; provider
policy inspection and shared independent validation remain separate dependencies.

The selected-space `GET /v1/spaces/{space}/status` now observes each currently
authorized query, document and staged reindex embedding binding through
`elephant_embeddings::provider::embedding_execution_recipe`. The same function
constructs the governed remote provider settings; local inference shares its
factory batch constant. The additive `embedding_execution_recipes` object is
visible only with **both** the existing status permission and `read:space_config`
authorization on that exact configuration record. Status-only callers do not
receive account or endpoint configuration. Revoked or absent bindings have no
recipe entry; reindex approval is not required for ordinary ingest readiness.

Each recipe includes actual configuration/binding/profile provenance, the full
vector partition, total physical HTTP attempt bound (null for local inference),
batch bound and versioned preprocessing segment/split settings. It never contains
credential references or values. An unsupported executable recipe is unavailable,
even if a stored approval exists. The handler verifies resolved configuration
identity and rereads the original full record before returning; this observation
does not replace the provider's required dispatch-time authorization.

Bootstrap captures each ordinary named-space status after worker startup under
its original HTTP and byte limits. It binds recipe provenance to the actual
created configuration and retains a hash for each complete recipe map. Boundary
rechecks refuse changed settings or worker instances without replacing the
original observations on a failed comparison. Original v1 receipt fields remain
unchanged; the original responses are already in their capture inventory.
