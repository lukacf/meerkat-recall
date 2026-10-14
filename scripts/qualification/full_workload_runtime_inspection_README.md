# Actual full-workload runtime inspections

`RuntimeRecipeInspector(client, plan, owner)` is the source consumer for the
actual managed worker, tree, retry and per-space embedding recipes. The verified
CLI must pin and execute both this module and the existing pure
`runtime_package_bootstrap_contract.py` from the original reviewed bytes. The
existing startup producer and this inspector share the same worker identity and
binding provenance checks. This source does not authorize execution or qualify a
product, provider, artifact, runtime or workload.

The plan supplies its original `backend`, `run_nonce`, both independently reviewed
`configurations`, and `worker_runtime_expectations`. The expectations format is
`recall-full-workload-worker-expectations/v1`, with the complete `work_queue`
(including all `doc_tree` fields), complete global `embed_worker`, and
`embedding_execution` for both named spaces. Each query/document factory supplies
all four physical execution limits. Missing values are refused. The full worker
recipe comes from the actual managed construction; per-space factories are
checked separately against the approved full partition, original configuration
ID, generation, binding and profile. Global embedding templates do not select or
authorize a per-space provider.

The CLI calls `await inspector.capture("initial")` before constructing the driver.
The same object's `capture` method is passed as `runtime_inspection`; the driver
calls `before_fw09_first_space`, `before_fw09_second_space`, then
`after_fw09_cleanup`, in that order. All return exactly `None`. Both named spaces
are checked at every boundary. A failed capture cannot be retried to adopt a new
worker or process. FW11 intentionally changes provider permissions after this
sequence and is not used to rewrite its original expectations.

The original owner exposes `client` and async `revalidate_original()`. It retains
the original process handles and raw process observations, returns the exact
`recall-full-workload-process-identity/v1` comparison projection, and refuses
unless the original runtime is active. The projection contains backend, nonce,
and `processes` with `recall` and `paired` (null for embedded). Each process carries
the original `executable_sha256` and the actual five-field F process identity:
`pid`, `ppid`, `pgrp`, `session`, `start_ticks`. Raw process evidence remains the
owner's responsibility; a caller-supplied PID or receipt is not an owner.

The same client supplies `runtime_request(body, response_limit=262144)`, restricted
to MCP initialization and `get_runtime_config`, using the original explicit
`runtime:inspect` JWT and no `X-Space-Id`. The actual server accepts a global
principal with no subject allowlist and `read:system` (also `write:system` or `*`).
Named config and status reads use each space's `manage` role with
`read:space_config`. HTTP framing, request counters, raw request/response files,
absolute deadline, wall expiry and capture reservation stay in the original
client. No second allocation is created.

The owner is checked before and after the public observations. The inspector
retains only a bounded comparison index in the same capture directory; original
HTTP and process bytes are neither rewritten nor reconstructed. Later checks
compare to the first complete observation, including the worker UUID and full
recipe hash, and both original configurations and factory hashes. Unavailable
workers, changed processes, incomplete responses, changed profiles and exhausted
original budgets refuse before dependent workload intake.

The storage-owned `processing.index_generation` is a content epoch: publication
and cleanup advance it without replacing approved processing policy. Initial
inspection requires its independently reviewed fresh value. Later inspections
retain its actual nonboolean unsigned value and reject regression, while omitting
only that field from stable policy comparison and hashing. The original raw
configuration response and the comparison index preserve each observed epoch;
they are never rewritten to the initial value. Configuration ID, processing
`generation`, bindings, profiles, connections, active profile, pending reindex,
security and subjects retain their original checks. The positive control changes
both spaces' content epochs across all four boundaries; changed policy, invalid
epochs and regression still refuse.

The closed-world control source tests both backends and all four boundaries,
recipe/partition/identity changes, omitted expectations, boolean/integer
substitution, original clock and capture exhaustion, incomplete HTTP framing,
global role isolation, and failed-capture non-adoption. Process creation and TCP
entry points are mocked to refuse. These controls establish adapter behavior
only; actual service execution and independent qualification remain separate.
