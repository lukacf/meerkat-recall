# Full-workload plan assembly

This package resolves the existing fourteen-case workload driver's inputs. It
does not start a service, construct a runtime allocation, contact a provider,
grant execution permission, or decide a qualification result.

`case-plan.json` pins the original independent contract, fifty-source corpus,
typed preparation contract, rendered map, E's Graph manifest and the component
bytes. The 24 workload Graph files retain exactly the original 56 source
identities. Each large Graph file remains separate and below eight MiB. The
assembler reads at most eight MiB per file and 32 MiB in total, and constructs
the driver's larger combined plan only in memory. Container-only recipes are
excluded from the full workload.

The component inventory pins all 18 executed code inputs from the CLI's actual
`MODULES` table: driver, transport, spy, Graph helper, load helper, CLI,
independent OB3 oracle, runtime owner, runtime inspector and its contract,
installed process helpers and their six original dependencies, and the pure
snapshot inspection reader. Each file's bytes and hash come from one immutable
composed source checkpoint, recorded as `component_source_checkpoint`.
The CLI verifies this entire set before executing any helper and uses the same
original byte snapshots. A changed helper requires an explicit pin refresh.
The current owner successor retains identity and journal capture tasks through
cancellation before cleanup. Its updated descriptor and package identity retain
the other 17 original helper descriptors and the approved candidate bytes.
Earlier package and test receipts remain evidence for their original source.
The selected driver consumes the explicit load-evidence hash, public failed
source cleanup and separate raw/rendered citation coordinates. These source
pins do not constitute independent acceptance of the candidate expectations.
The required ninth citation field is `doc_id`, a canonical 40-byte
`doc:<UUID>` string. The driver compares it with the original accepted document
and the actual immutable revision's parent. It accepts no alternate alias and
never derives this identity from an external source key or recipe template.

Independent qualification owns the additional literal expectation file. The
assembler requires its separately reviewed file SHA-256; it never learns
expectations from observed output. Its required shape is:

```json
{
  "format": "recall-full-workload-plan-expectations/v1",
  "contract_sha256": "44a91b9f0ae4c2ae32d2f8aec9eb783a48ccca63eeee010ff2427ef1ec4a0c0b",
  "graph_manifest_sha256": "1676812ec959f62700f46e38158143f07f00a317e79b64f616d7ab0391695da8",
  "configurations": {},
  "worker_runtime_expectations": {},
  "runtime_principal_templates": {},
  "cases": {},
  "load_plan": {}
}
```

The empty maps above describe required input locations and deliberately fail
validation. Both named configurations must supply the driver's full independent
`configuration`, `subject`, and `grant` expectations. `cases` must contain all
FW01–FW14 entries and the exact fields consumed by `full_workload_driver.py`.
`load_plan` must contain 280 literal lexical requests, exactly two per each of
140 caller IDs, with complete independently reviewed revision coverage. Existing
50-caller semantic queries are not silently converted into this different plan.
No missing field receives a permissive default. Unknown terminal-error or
provider-retry expectations must be resolved by their independent owner before
assembly succeeds.

`worker_runtime_expectations` is required in the independently pinned input and
is copied into the resolved plan without deriving any values from the runtime.
It contains format `recall-full-workload-worker-expectations/v1`, the complete
`work_queue` including `doc_tree`, all five `embed_worker` fields, and
`embedding_execution` for both named spaces and both query/document operations.
Each operation includes `max_total_http_attempts`, `max_inputs_per_call`,
`initial_segment_bytes`, and `max_split_depth`. The candidate contains the full
concrete input example; missing or extra fields and wrong scalar types refuse
assembly. Structural validation does not approve the proposed literal values.

The proposed queue retains one concurrent worker and one concurrent extraction,
three transient attempts, and 10/120-second retry bounds. Its other full recipe
values are explicit inputs. The global embed template explicitly uses provider
`unconfigured`, model `text-embedding-3-large`, and 3072 dimensions; enabled
workers still obtain their actual per-space query/document factories from the
governed configuration. Those spaces retain their existing small/large model
and dimension choices. The owner must explicitly construct the reviewed
configuration and the inspector must compare actual full recipe values; neither
an absent environment variable nor a health response supplies an expectation.

The final inspection callback is `runtime_inspection=inspector.capture`, with
`capture(boundary: str) -> None`. It observes the same original processes,
worker instance, full recipe, both named configurations, and factory execution
limits at `initial`, `before_fw09_first_space`, `before_fw09_second_space`, and
`after_fw09_cleanup`. The CLI performs the initial capture and the driver
requires the remaining three. Failed or missing observations refuse the run;
the callback is not an acceptance receipt. The same original deadline, HTTP
counter, and capture budget apply at all four boundaries.

`candidate-expectations.json` is the producer's concrete proposal submitted for
that independent review. It carries no runtime authority. The separate
`independent_runtime_input_review` descriptor pins qualification's exact
pre-output worker, principal and fake-credential review at
`7ca07c214128221f4fb3decb90cbdb481de0237d`. That review approves those input
literals; it does not approve the whole workload, helper family or execution.
The approved candidate bytes remain unchanged, including their original
submission notes. Whole-family review and runtime evidence remain separate.
The candidate contains
all fourteen cases, both configuration templates, literal evidence requirements
from the original independent facts, and 280 new lexical request intents. Its
`derivation` and `pending_independent_decisions` fields identify every unresolved
case obligation. In particular, the three original 503 responses do not imply
eventual successful publication, and current spy inactivity does not establish
that the work queue is drained. FW09 ordering/cleanup and FW11 residual retry
disposition need an independent decision and corresponding driver behavior.

The literal lifecycle queries include `required_evidence`, because coverage
alone cannot prove a surviving shared claim. The L query consumer must check the
actual result kind, source, revision, full original quote, digest and byte span
before these cases can qualify. Unknown future record IDs are never input
expectations. FW13/FW14 require actual external receipts; the original 56-source
corpus has no approved guaranteed oversized-response case to synthesize locally.

FW12 also supplies `query_requirements` keyed by all 280 original request IDs.
Its required `query_requirements_sha256` is the SHA-256 of that map encoded with
sorted keys, compact JSON separators, UTF-8, and no ASCII escaping. Assembly and
the load consumer both compare this exact independent pin before execution;
the driver passes it unchanged with the same map to `run_load`.
Each retains a full independently authored quote and digest for its requested
source/revision; Graph queries additionally require that evidence on an actual
claim. The load driver must call the same source-bound evidence validator used
by ordinary workload queries. Coverage, an empty result list, or an unrelated
claim cannot establish this known-positive protocol check. No query or warmup
is added, and this remains separate from semantic-quality or latency approval.

The cleanup expectations distinguish historical receipts from current heads.
The current public wire maps a superseded receipt to `state: failed_final`,
`error_code: superseded`, and historical `lifecycle: active`. Its identity stays
the original operation/generation. The new retraction receipt and current head
must separately report `lifecycle: retracted`. The proposed effective worker
profile and failed-first ordering come from qualification's approved fixture
control direction; actual effective process configuration and public state
observations remain required. FW09 retracts the failed original before releasing
the held running call. FW11 retracts residual original work before restoring its
binding. A zero count of currently active spy connections proves neither drain
nor cancellation; each original rule's later sends must remain absent.

The owner loads this module and calls:

```python
plan, plan_sha256, provenance = assemble(
    package,
    {"qualification": qualification_checkout,
     "recipes": graph_recipe_checkout,
     "components": reviewed_driver_checkout},
    {"root": "qualification",
     "path": "tests/recall_preview/full-workload-plan-expectations.json",
     "sha256": independently_reviewed_expectation_file_sha256},
)
```

Pass `plan` and `plan_sha256` to the existing L driver after its ordinary
`validate_inputs(contract, corpus, plan, plan_sha256)` check. Preserve the source
provenance separately from actual runtime receipts. All three roots may point
to one integrated source checkout when its exact pinned files are present.
Component updates require an explicit reviewed pin update; dirty or newer files
are not assumed compatible. Recomputing a hash does not confer owner approval.

To use the existing command-line loader, call
`document, identity = driver_input_document(plan, plan_sha256, provenance)`.
It replaces only `graph_recipes` values with the CLI's existing
`{"file": {"path": ..., "sha256": ...}}` references to the original files
that assembly actually read. The original resolved plan hash stays unchanged.
Persist exactly `encode(document)` in the owner's capture/input area; pin its
`identity["document_sha256"]` in both manifest and allocation `case_plan`
descriptors, and retain `identity["resolved_plan_sha256"]` as the allocation's
`resolved_plan_sha256`. These are source identities, not an execution grant.
The helper returns detached values and writes no files. The source document
must fit eight MiB; its document-plus-recipe bytes must fit 32 MiB. The CLI
additionally counts every actual allocation, token, receipt and other input
against that same aggregate limit. It reopens and hashes each original recipe;
a changed recipe cannot resolve to the pinned plan.

`test_composed_inputs.py` is the central integration control for the exact
pinned package. It loads actual reviewed helper bytes inside process/network
guards, assembles the real inputs, resolves all 24 Graph references through the
CLI reader, and checks both backend input bindings against the same approved
principal templates. Allocation, artifact and path values are explicitly
synthetic with execution disabled; no runtime owner is started. Run it in the
complete source composition, or set `RECALL_FULL_WORKLOAD_SOURCE_ROOT` to that
checkout. It is separate from runtime, installed-artifact and provider tests.

`configuration_requests(candidate, owned_endpoints)` fills only each original
`owned-spy` connection's endpoint in a copy of the source templates. Both actual
IPv4 loopback HTTP endpoints are mandatory. The configuration owner retains the
original templates, endpoint allocation, and bound request hashes before using
the ordinary signed administration routes. The helper neither allocates ports
nor sends requests. Current generated configuration/grant IDs, generations and
timestamps are observed later and never fabricated in this source package.

The owned-runtime constructor consumes a separately pinned **pre-start**
`recall-full-workload-runtime-plan/v1` document. Build it with
`runtime_plan_document(candidate, runtime_inputs, original_spy_recipe)`. The
required `runtime_inputs` shape is the following complete Python example using
values supplied by the original allocation owner:

```python
runtime_inputs = {
    "backend": approved_backend,
    "run_nonce": original_run_nonce,
    "endpoint": original_recall_endpoint,
    "spy_endpoint": original_spy_endpoint,
    "run_directory": original_new_run_directory,
    "archive": reviewed_release_archive_path,
    "artifact_sha256": reviewed_archive_sha256,
    "binary_sha256": reviewed_binary_sha256,
    "source_commit": reviewed_source_commit,
    "target": reviewed_target,
    "paired": original_paired_input,  # None for elephant-db; see below.
    "spy_credential": "synthetic-controlled-provider-key",
}
runtime_document, runtime_sha256 = runtime_plan_document(
    candidate, runtime_inputs, original_spy_recipe,
)
```

For `surreal-db`, `paired` is exactly `{"binary": reviewed_paired_binary_path,
"binary_sha256": reviewed_paired_binary_sha256, "endpoint": original_paired_endpoint}`.
The helper binds both CreateSpace requests as a list in the original household,
business order, the exact worker expectations, and per-space `data`, `manage`,
and `revocable` principal requests. Data scopes and subject come from the
original grant expectations. Explicit management scopes cover configuration,
grant, provider and status routes; the separate read-scoped revocable subjects
match the original FW10 cases. Real startup must issue every signed JWT and
create every active grant through the actual supported routes. No token path,
worker UUID, PID, future receipt or startup-success claim is an input substitute.
The resolved case plan also carries a detached copy of the same reviewed
`runtime_principal_templates`. Before startup, the CLI must compare the exact
management and revocable subjects and scopes with those originals, alongside
the existing data grant and FW10 subject checks. The runtime plan's hash does
not substitute for this binding to the independently reviewed case inputs.

The fake credential is the original literal in E's independently pinned Graph
manifest. Its raw UTF-8 digest must match every original spy rule; inventing a
different prefix or rewriting those rules is rejected. The helper returns a
detached document and hash without touching disk or starting anything. The CLI
must bind it to the same original allocation and case plan before the runtime
owner verifies artifacts, new paths, endpoint custody, current process identity,
and shared resource limits. These source inputs do not authorize execution.

The frozen 1,800 seconds/backend, 8,000 HTTP calls, 4,096 spy calls, one GiB
capture, two-million-byte source limits, sequential backends, zero external
provider sends and zero paid calls remain unchanged. The package lists required
actual allocation, signed caller, artifact, endpoint, capacity-export, MCP and
external-consumer inputs. These are not populated with invented IDs or pass
fields. FW13/FW14 remain externally owned; public absence does not establish
physical erasure or bounded backup retention.

The candidate's two fresh named spaces explicitly select the new immutable
`embedding-full-v2` profile with `preprocessing: "text-full-v2"`. New
`query_embedding-full-v2` and `document_embedding-full-v2` bindings point to
that same active profile. The independently selected small/large models,
1536/3072 dimensions and controlled model versions remain unchanged. The
`graph-v1` generative profile retains `text-v1`. Source owners reviewed this
fresh-space configuration against the actual governed constructor and profile
partition contract; independent full-plan review and runtime qualification
remain required. Existing paid profiles, configurations and receipts retain
their original bytes. Activating v2 on an existing space still requires its
actual checked reindex transition; these fresh-space templates cannot replace
that transition. The controlled spy does not establish semantic model quality.

`external-consumer-input.schema.json` describes the exact external receipt
descriptors in both the original run manifest and allocation. The owner supplies
`owner: "OB3"`, an `original` descriptor, and a separate `expectations`
descriptor. Each file descriptor contains only its actual canonical absolute
`path` and original raw-byte `sha256`. The manifest and allocation must contain
the same whole descriptor; a receipt cannot nominate a different expectation.
The CLI requires these consumer input files to be owned by the current task
user with no group or other permissions, and retains the opened inode's identity
through the read. Source recipe files use the separate ordinary input reader.
Each input is limited to four MiB, and both count toward the existing 32 MiB
input and one GiB capture bounds. Neither raw file is re-encoded for the oracle.

The expectation file uses the independent oracle's
`recall-ob3-outage-expectations/v1` contract: original `run_id`, `ob3_git_sha`,
`recall_contract_version`, `recall_endpoint_state`, `batch_kind`,
`requested_worker_ids`, `expected_participating_workers`, `por_baseline`,
`maximum_logical_reads`, `earliest_started_at`, and `latest_finished_at`, plus
`format`. These values must come from the independently approved original
recipe, controller interval and POR baseline. This package supplies no such
runtime values. The exact schema and semantic predicates remain owned by
[`ob3_outage_oracle.py`](../../../scripts/qualification/ob3_outage_oracle.py).
The CLI retains the two bounded original byte strings; the driver calls
`evaluate_receipt_bytes(original_raw, expectations_raw)` without substituting
JSON serialized from a parsed object. Capture must retain both original files
under the existing reservation and record their hashes separately.

An older `{owner, original}` descriptor still denotes a historical import.
Without the independently approved original expectation bytes its consistency
gate remains unmet. Original v1 receipts also remain unmet under the new v2
oracle. Even a passing scoped consistency result does not prove FW13 physical
size-retry traces, FW14 installed twin execution, physical erasure, backup
retention, or whole-product acceptance.

`test_assemble.py` contains closed-world input-integrity and finite-bound
controls. They use synthetic unit data, scratch files and disabled network and
process constructors, and provide no product or provider qualification.

The headerless embedding successor adds optional `spy_credentials` to the
pre-start runtime inputs and `controlled_spy_credentials` to the independently
reviewed expectation document. Both maps name exactly the original two spaces.
Runtime entries contain `credential_ref` and `value`; expectation entries also
contain the SHA-256 of that fixed fake value. The assembler checks both maps,
every connection reference, and every spy rule's declared space and credential
digest. A credential/model/path tuple spanning spaces refuses before startup,
as does a tuple matching multiple embedding rules. Multiple generation source
rules in the same declared space remain supported; text cannot establish scope.
The runtime owner independently checks the exact fake literals and exports only
the two per-space key variables, without the shared-key fallback.

This is a new immutable input materialization: the original graph manifest,
all graph sources and semantic expectations remain retained. Only the successor
workload spy's credential hashes, the corresponding manifest hashes, and the
configuration credential references change. Container-spy inputs do not change.
The original `spy_credential` field and plans without the optional map remain
available for historical controlled fixtures. A legacy shared-key plan is not
evidence that the successor workload used distinct keys or qualified embeddings.
