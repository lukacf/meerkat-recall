# Independent dynamic runtime and D28 preparation contract

This is D's normative pre-output predicate contract for a separately versioned
v2 consumer. L owns its literal pure validator under
`tests/recall_preview_semantics/dynamic_component_validator.py`; D must review
that exact implementation and transitive source hashes before any activation.
The existing v1 paths remain fail-closed. This document grants no provider,
namespace, process or artifact execution and changes no existing allocation.

The pure interfaces are:

```python
validate_runtime(reviewed_static, observed_runtime, raw_evidence, controller_context) -> dict
validate_prepared(reviewed_static, observed_preparation, runtime_component,
                  raw_evidence, controller_context) -> dict
```

`raw_evidence` is a bounded `Mapping[str, bytes]` (or the caller's equivalent
deterministic reference reader) from exact SHA-256 to original bounded bytes,
never reconstructed JSON. The validator owns no I/O; a caller-supplied lazy
mapping may load an immutable original file at the caller boundary. Never copy
the entire mapping or materialize the provider channel into a dictionary.
Hash, length and kind are independently checked on every lookup, including
repeat reads. The caller owns immutable file custody and a cache of at most
256 MiB; parse one bounded provider record at a time, discard request/response
payloads after its predicates, and retain only compact dispatch identities.
The original provider disk channel remains at most 20 GiB, with at most 16384
references and 64 MiB per raw record, further restricted by every original
request/response/channel subcap. Unique runtime evidence is capped at 352 MiB
(current capture 148 MiB, installed capture 96 MiB, plus bounded original static
inputs/carriers). These are inventory bounds, not permission to allocate a
352 MiB or 20 GiB in-memory dictionary. Verify unique aggregate inventory bytes. Every reference is exactly `{sha256, bytes, kind}`; a missing, duplicated,
wrong-kind, wrong-size or wrong-hash reference refuses. Parsed JSON rejects
duplicate keys, nonfinite numbers, booleans masquerading as integers and unknown
fields. Validators perform no I/O or process/network operations. Failure raises
`ValueError` with a safe static code; no partially qualified component is returned.
Context verification and current process observations are performed by the
original reviewed G/C caller and supplied as raw hash-bound evidence, not
accepted solely because a field says `qualified` or `passed`.

`reviewed_static` is fixed before launch. It contains exactly the fields listed
in `dynamic-component-contract.json`: original source/artifact/runtime/helper
hashes, backend and finite bounds, the original G allocation/controller inputs,
the static bootstrap request plan, original workload/corpus/typed-delta bytes,
independent rendered-map bytes, runtime recipe and required installed-case
inventory. It contains **no future generated configuration ID, receipt hash,
process PID, capture path/hash, prepared inventory or dynamic component hash**.
The contract's own raw hash is part of the ordinary pre-output D source review.
Keep the existing acyclic R → P → T → D → inputs → S → G → actual C/B evidence
chain; dynamic receipts cannot become prerequisites of their own static review.

The observed runtime envelope is `recall-dynamic-runtime-evidence/v2`. Its exact
fields are `format`, `identity`, `bootstrap`, `installed_offline`,
`actual_runtime_configuration`, `actual_space_configurations`, `controller`,
`current_processes`, `capture_inventory`, `command_inventory`, `outer_launch` and
`cleanup_obligations`. All except `format` and `identity` are raw-evidence
references. `identity` has exactly `source_commit`, `artifact_sha256`,
`binary_sha256`, `runtime_sha256`, `paired_artifact_sha256`,
`outer_configuration_sha256`, `backend`, `endpoint` and `run_nonce`.
Every static runtime identity except `outer_configuration_sha256` must equal
its original reviewed input. The outer configuration is a downstream original
caller input: bind its actual raw hash to the unchanged G anchor and reviewed
caller manifest/spec and input inventory during observation. Do not require a
future generated outer hash inside its own prerequisite review.
`paired_artifact_sha256` is the original real 64-hex paired **executable** digest
for BOTH backends, matching G's actual `recall-surreal` binary hash and runtime
package binary entry. This corrects D's earlier archive wording after auditing
the original producer; the original field and bytes do not change. Its presence
does not prove paired execution or archive provenance. Those remain separate
artifact gates. Never substitute null, synthesize an unused identity, or
normalize already captured bytes.

The v2 `artifact_sha256` identifies the reviewed installed archive, while
`binary_sha256` identifies the actual executable. The existing C v1 bootstrap's
`artifact_sha256` means executable SHA: compare it to v2 `binary_sha256`, not v2
archive SHA. Bind the archive and executable through the original installed
artifact manifest and actual offline command/capture evidence. No field rename
or equality assumption may erase that distinction.

Runtime validation requires all of the following:

1. C's original bootstrap remains `recall-runtime-bootstrap/v1`,
   `scope: authenticated-clean-persistent-startup-only`, `qualified: false`,
   `product_acceptance: false`. Its actual original request/response captures
   establish fresh CLI auth, both exact named configurations, current grants,
   both wrong-space denials, bootstrap data denial, read-only write denial,
   revocation/restoration, clean source inventory and semantic worker readiness.
   No adapter may rewrite this receipt into installed qualification.
2. Actual installed **offline prerequisite evidence** must already pass D's
   frozen 19-case `installed_linux_cases.json` predicates for the exact candidate
   source/archive/binary/backend and independently reviewed offline recipe. Its original driver/transport/
   process-owner hashes, command journal, complete response captures, process
   custody and terminal cleanup must be present. A case-name list or observation
   string is insufficient. Required cases are preserved verbatim by the JSON
   contract. Controlled provider-dispatch qualification remains separately
   scoped; fixed vectors cannot establish relevance or preparation quality.
3. If an earlier isolated offline artifact run is reused, it retains its OWN
   original reviewed controller/isolation proof and clean terminal receipt.
   It cannot impersonate the current G controller. Exact source, installed archive,
   executable and requested backend must match. The original installed 19-case
   lane has only those four identities and uses in-process rocksdb for its
   native backend; its original case file explicitly excludes remote patched
   SurrealDB and active provider acceptance. Current C runtime separately proves
   its full paired package/executable, runtime, controller and recipe identities.
   Never invent paired/runtime/run-nonce fields in the offline record. Exact
   installed controlled-provider and paired-server qualification remain separate
   required actual runs. This corrects the overbroad offline equality proposed
   in D0bb62531, after original-producer audit and root's 03:11 scope ACK. Each launch's configuration is checked against its OWN original reviewed
   recipe. The installed 19-case fixture intentionally uses unapproved Metadata
   configuration; it need not have the current semantic launch's provider
   bindings, approvals, generated named-space IDs or endpoints. Never rewrite
   that fixture to pretend it exercised approved providers. Original isolated
   paths, ports, principals and fixture configuration may differ when both exact
   reviewed recipes account for those differences. Current actual IDs/timestamps
   and approved configurations come solely from C's current captures. Artifact
   version/name equality or a case-name list cannot establish compatibility.
4. The actual saved runtime configuration must establish the frozen retry and
   tree recipe (`transient_retry_max_attempts=3`,
   `embedding_max_total_http_attempts=3`, `document_tree_create_summaries=false`)
   and exact persistent backend/media/journal paths from the reviewed plan.
   Bind complete original bytes; do not infer these settings from health/readiness.
5. Actual named configurations match the static creation requests for every
   requested field: space, policy/level, purpose/skill, provider/model/dimensions,
   connections/accounts/endpoints, credential references/generations, immutable
   profiles, operation approvals and processing generation. Only server-generated
   IDs/timestamps/creator/provenance/subjects/deletion fields are dynamic, with
   the exact fresh-state predicates already frozen by C. Bind the full actual
   records and their raw capture hash. Never invent generated values before output.
6. Current service/native-server process executable hashes, PID/PPID/group/session/
   start ticks and original commands match the owned original G namespace and
   current C process journal. The same original controller identity, run nonce,
   monotonic start, backend phase start and whole deadline remain in force.
   Reading a new clock cannot reset a deadline. G's actual read-only input,
   namespace and capability evidence must match its original reviewed anchor.
7. Every completed owned setup/CLI process was reaped successfully with bounded
   logs. Current persistent services remain explicitly owned and live with
   unchanged identity. Their later cleanup remains a terminal obligation: any
   forced kill, nonzero service exit, missing reaping or failed signal/log drain
   prevents the combined final acceptance and suppresses a next backend. An
   earlier successful bootstrap receipt stays immutable; cleanup is separate.

Successful runtime validation returns `recall-installed-runtime-component/v2`
with exact fields `format`, `verdict: passed`, `scope: installed-offline-and-current-runtime`,
`product_acceptance: false`, `identity`, `static_review_sha256`,
`contract_sha256`, `evidence_sha256`, `controller_sha256`,
`actual_runtime_configuration_sha256`, `actual_space_configurations_sha256`,
`required_cases`, `remaining_cleanup_obligations` and `proof_limits`.
The last two fields are never erased to make a prerequisite appear terminal.
No standalone `qualified` boolean substitutes for this complete component.

The preparation input is `recall-dynamic-preparation-evidence/v2`, with exact
fields `format`, `identity`, `runtime_component`, `observation`,
`capture_inventory`, `configuration_before`, `configuration_after`,
`provider_cohort_begin`, `provider_accounting`, `provider_accounting_channel`,
`controller`, `original_intents`
and `immutable_revisions`. Reference fields bind original bytes. `identity` is
the runtime identity plus `corpus_sha256`, `workload_contract_sha256`,
`preparation_contract_sha256`, `typed_delta_sha256`, `rendered_map_sha256`,
`provider_allocation_sha256` and the fresh `preparation_nonce`. A backend/phase
or cohort nonce cannot be substituted merely because the artifact is the same.

Preparation validation requires:

- The exact validated current runtime component and its complete evidence chain;
  no source acceptance may precede the installed prerequisite or original G
  preparation cohort opening. Actual current process custody is rechecked at
  the consumer boundary. Completion must precede the original phase/whole deadline.
- Exactly 50 original typed Retrieval Source+Doc submissions, 25 per named space,
  each preceded by a durable original request intent before its sole physical
  send. Original 50 absence responses and both original configs precede the
  first submission. No warmup, retry, replacement, adoption or repaired fixture.
- Every original accepted operation/source/generation/doc/work identity is bound
  to the actual typed POST response; polls and current heads retain that exact
  identity. All 50 current heads and original operations are complete, active,
  published at the exact frozen revision with no failure. Pending is not success.
- Full Source input hash/URI, original typed classified envelopes and unknown
  subjects remain exact. Raw source hashes stay separate from rendered evidence.
  Every original Doc, immutable DocRevision, node and pipeline response is bound
  to a complete original capture. IDs and parent links agree, current publication
  remains the originally completed revision, and the new public immutable read
  retains exact original text/hash/renderer metadata and the required raw-read
  authority. No private manifest/completion receipt is synthesized.
- Exact rendered bytes and root/paragraph/chunk spans equal D's independently
  pinned map, including final LF only in the root. Complete public node inventories
  preserve source+immutable revision lineage, policy and subjects. The original
  finite node bound, complete count/profile markers and actual pipeline coverage
  predicates remain. These are public publication markers, not raw vector values.
- Full before/after named configurations may differ only in the original
  storage-owned `processing.index_generation` and `updated_at`; all authority,
  provider/account/model/profile/binding/credential generation fields remain exact.
- G's original preparation begin/close receipts, baseline/after/capture/sealed
  hashes, controller and actual provider ledger match the original allocation,
  backend and cohort. Admission is closed and unsettled dispatches are zero.
  Preserve the existing 450 preparation-attempt cap per backend, 225 per space,
  concurrency 2, 3600 seconds, exact 1,064,960 nano-USD no-refund reservation per
  physical attempt, and original whole-allocation bounds. No retries refund
  attempts. `per_request_association: unmet` remains explicit; no fabricated
  public-call to physical-provider-call attribution.
- All capture bounds and original clocks hold. Any missing, incomplete, oversized,
  failed or cancelled request/cohort record refuses qualification. A producer's
  `capture_verdict: passed` is an observation to verify, not an independent verdict.

Successful preparation returns `recall-prepared-fixture-component/v2` with exact
fields `format`, `verdict: passed`, `scope: prepared-public-retrieval-fixture`,
`product_acceptance: false`, `identity`, `static_review_sha256`, `contract_sha256`,
`evidence_sha256`, `runtime_component_sha256`, `controller_sha256`,
`actual_space_configurations_sha256`, `source_count: 50`,
`sources_per_space`, `published_inventory`, `provider_accounting_sha256`,
`remaining_cleanup_obligations` and `proof_limits`.

S's separately versioned measurement consumer must verify this entire component
and original raw evidence chain, then recheck current original process/config/head
identity before opening G's distinct measurement cohort. It preserves the frozen
200 measured requests, 50 callers × 4, latency/quality/byte thresholds, exact
question schedule and all original budgets. Nothing here is a semantic pass.

The authorized immutable DocRevision GET is an additive **inspection** request.
For D28, one per source adds 50 HTTP calls: the minimum preparation path changes
from 404 to 454 calls (four config reads, 50 absences, 50 accepts, 50 successful
operation polls, 250 existing publication reads, 50 immutable-revision reads).
The original total remains 4096, leaving at most 3642 additional operation polls
and other already-budgeted calls. Request accounting is global and pre-send;
the original 256 MiB private capture, 262144-byte response, 10-second request and
3600-second preparation limits remain. No truncation or reserve oversubscription.
Primary collection similarly counts its one revision read per original source
inside its existing HTTP/capture/deadline cap; it does not alter measured queries
or paid physical-attempt allocations. C owns the exact route/response/error
contract; consumers must wait for its frozen source, never guess response fields.

## Original validation and current preparation custody

The preparation envelope's `runtime_component` reference has kind
`runtime-validation-chain` and exact raw shape:

```text
{format: "recall-dynamic-runtime-chain/v2",
 component: ref(kind="runtime-component"),
 evidence: ref(kind="runtime-evidence"),
 validated_monotonic_ns, validated_wall,
 current_custody: ref(kind="runtime-current-custody")}
```

The supplied `runtime_component` argument must equal the original component
object. S captures the original validation clock samples immediately after
successful original runtime validation, before any preparation intent/POST, and
retains them unchanged. Every first physical submission starts at or after that
sample. Revalidate the original runtime evidence using its original G controller
bytes and those original samples, and require the exact same component. A later
recapture cannot stand in for a prerequisite that had to pass before submission.

`current_custody` has exactly `format`, `current_processes`,
`capture_inventory`, `cleanup_obligations`, `worker_recipe_capture`,
`provider_recipe_captures` and `outer_launch`. Its format is
`recall-dynamic-current-runtime-custody/v2`. The first three values are existing
raw references with kinds `current-processes`, `capture-inventory` and
`cleanup-obligations`. `worker_recipe_capture` is a pair, and
`provider_recipe_captures` maps exactly the two original named spaces to pairs.
Each pair is exactly `{request_index, request, response}`, with the two original
raw references of kinds `startup-request` and `startup-response`. The index must
match the original C request/capture row; both files must occur in the current
inventory. This includes actual revalidation calls made after the frozen
startup receipt. Do not mutate the original startup receipt or validate only
its older request list.

C observes the current managed worker and both per-space provider recipes,
then captures original process custody. L independently parses those actual
authorized get_runtime_config/status responses. The original worker instance
and complete recipe must remain identical; process liveness alone cannot prove
that a worker thread remained the same. Current provider recipes must retain
exact original factory settings, provenance and partitions. The boundary
request/response intervals follow the source preparation observation; process
observations follow these calls and precede or equal the current controller
sample. The current sample has the same original controller/boot/anchor/run
nonce and deadline. Outer `configuration_after` and original publication/head
captures retain their existing authority; this carrier adds no duplicate head
or configuration state. Only the previously permitted storage index generation
and update timestamp changes are allowed in named configurations.

For equal original byte hashes, use canonical kinds in the new v2 inventories:
original and forwarded provider request files both use `provider-request`;
other provider files use `provider-dispatch-intent`, `provider-response` and
`provider-capture`. Paths and original send-intent hashes retain the distinct
request roles even when the original bytes are equal. All ordinary original stdout and
stderr logs use `process-log`; original process stat samples use `process-stat`.
These new inventory labels do not rewrite old raw records or legacy receipts.
Unknown cross-kind aliases still refuse. Provider accounting bundle kinds remain
`provider-accounting`, `provider-cohort-begin`, `provider-cohort-close`,
`provider-baseline`, `provider-phase-clock`, `provider-sealed`,
`provider-capture-inventory` and `provider-dispatch-records`, with hyphens.


## Original static launch anchor

`controller_policy.expected_anchor` has exactly these 15 keys: `format`,
`run_nonce`, `source_commit`, `proposal_sha256`, `corpus_sha256`,
`allocation_sha256`, `artifact_sha256`, `paired_artifact_sha256`,
`runtime_sha256`, `gateway_sha256`, `bindings_sha256`, `bridge_sha256`,
`context_helper_sha256`, `command_sha256`, `caps`. The format is the original
`recall-gateway-launch-anchor/v1`; caps, original approved proposal/allocation,
helper bytes and executable identities must match exactly. Allocation is null
only for the original primary route. D28 retains 2100 physical dispatches and
3,000,000,000 nano-USD, with the original whole 7800-second clock bound.

Do not accept an arbitrary subset. The final `manifest_sha256`,
`configuration_sha256`, `input_inventory_sha256`, `clock` and `parent` are
forbidden in this pre-output expected anchor. They are still required in the
original observed full anchor and independently bound to original caller
manifest/spec/input bytes and actual launch clock/process custody. Excluding
them here removes a circular prerequisite, not any downstream evidence check.
C passes this exact stable object unchanged to original G context verification.
Legacy primary launch expectations retain their original separate schema.

## Original read-only accounting channel

The preparation v2 envelope adds the required `provider_accounting_channel`
reference of kind `provider-accounting-channel`. Its raw object is exactly
`{format, anchor_sha256, sidecar, mountinfo, index, directory, caller}` with format
`recall-provider-accounting-channel/v2`; the latter five fields are references
with the exact kinds and byte ceilings in the JSON contract. `anchor_sha256`
equals the original validated G anchor hash and the original sidecar's anchor
hash. Every original smaller channel bound remains effective. These bytes count
in the existing total inventory; this creates no new disk/cache allowance.

G captures the actual directory `lstat` once during the same original
`observe_channel` call, checks its existing effective-user/private-mode guard,
and returns those original facts as JSON with exactly
`{format, path, device, inode, uid, mode}` and format
`recall-provider-accounting-directory/v2`. `path` is `/gateway-accounting`;
other facts are the existing original `directory_identity` result. All numeric
facts are nonnegative integers, never booleans. S wraps the four original byte
observations returned in `channel` with typed references; it must not reconstruct
the directory observation from the sidecar or capture it later. The exporter
still returns its four existing keys `reference`, `raw_evidence`, `files`,
`channel`; only `channel` gains `directory` beside its original three raw values.

L owns this channel predicate. Independently compare the observed directory
facts to the original sidecar's exact `directory` object, path and pinned reader
source hash; require private permission bits and the original actual mountinfo's
single exact mount path, matching device, `ro` per-mount flag and absence of
`rw`. Source-pinned G acquisition establishes the effective-user check within
original verified caller/namespace custody; there is no UID field in the legacy
controller identity to invent or synthesize. L must describe that scope honestly:
a sidecar assertion alone does not observe an inode, owner or mount. This is
original evidence acquired by the reviewed helper, not a cryptographic proof
against a malicious observation process.

Parse the original G index under its exact v1 schema. Its cohort/backend/phase,
canonical original caller hash, original begin/close hashes and `reference`
must match the actual preparation records and unchanged `provider_accounting`
reference. Bind every original file entry, unique membership, kind, size and
hash against the same bounded lazy original-byte Mapping. No ambient SQLite,
credential file, mutable caller path or reconstructed ledger is evidence.

F continues to receive the original fixed eight-field provider accounting bundle
and its unchanged API. No ninth bundle field, new authority, trusted success
boolean or alias is introduced. L passes the original accounting reference to
F only after checking channel custody and preserves F's original ledger,
transport, request/response and budget predicates.


The channel's required `caller` is a `workload-phase-manifest` reference to the
original `preparation.manifest.json` saved before the first cohort begin. S must
retain that exact original file reference in the finite evidence inventory even
though the file resides outside the preparation subdirectory. Never reconstruct
the manifest from a later observation, nonce or reviewed static object. Strictly
parse its original bytes and compare the SHA256 of G's canonical encoding of
that original parsed object to G's index `caller_sha256`. Separately verify the
typed reference against the exact saved raw bytes. These are distinct checks;
never replace original bytes or assume raw-byte and canonical-object digests
are interchangeable. Bind its actual phase/backend/source,
executable, corpus, allocation, preparation nonce and contract identities to
the original preparation and approved inputs; authorization flags alone grant
no authority.

The caller cap is **131072 bytes**, applying G's existing private-input file
ceiling to this newly explicit carrier. Source audit: S's generic `OriginalFiles`
ceiling is 64 MiB; G's 16384-byte control request contains only the caller digest,
so that control cap is not a previously enforced whole-manifest cap. This new
131072-byte manifest check tightens the generic bound, never enlarges an
existing allowance. It remains inside existing total inventory limits. No
additional manifest copies, dispatches or provider budget are authorized.


## Original downstream launch files

Both the runtime envelope and later current-custody carrier require `outer_launch`,
a reference of kind `gateway-launch-inputs`. Its original derived object has
exactly `{format, manifest, spec, input_inventory}` and format
`recall-gateway-launch-inputs/v2`. These three fields are typed raw references
of kinds `gateway-manifest`, `gateway-launch-spec` and `input-inventory`.
The wrapper is at most 16384 bytes. Manifest and spec each retain G's original
131072-byte private-file ceiling; inventory retains its existing 8 MiB reader
ceiling and G's stricter 256-file/8-MiB-per-file/32-MiB-tree limits.

G exposes only the original already-persisted `run_directory/manifest.json` at
fixed read-only `/gateway-manifest.json` and `run_directory/launch.json` at fixed
read-only `/gateway-launch.json`. It derives the original parent paths under
its existing owned run-directory authority; callers choose neither source nor
child paths. No approval file, provider key or new configuration object is
mounted. The launch spec **is** the outer configuration. The preparation caller
manifest already carried in the accounting channel is a different original file.

C reads these exact two bounded raw files during both initial and current
custody acquisition and records them in the corresponding capture inventories.
It reuses the exact original `current_processes.input_inventory` reference in
the wrapper, rather than creating a second inventory authority. S carries these
original references and bytes without reconstructing them from the anchor.
Every wrapper/member reference must be inventoried. Current wrapper and raw
identities must equal the initial originals; a later capture cannot replace an
initial precondition. Existing aggregate capture bounds remain unchanged.

L independently hashes the raw manifest and spec against the unchanged full
anchor's `manifest_sha256` and `configuration_sha256`. The spec hash must also
equal the runtime identity's `outer_configuration_sha256`. Check the exact
original G manifest/spec schemas and relevant values: run nonce, executable,
runtime/gateway/corpus/allocation identities, original endpoint/account/bindings,
exact command and permitted bounded environment, and input inventory. The
manifest's configuration hash must equal the spec raw hash; manifest/spec
bindings must match and their G canonical hash must equal `bindings_sha256`;
the original command's G canonical hash must equal `command_sha256`. Spec input
inventory hash, unchanged anchor inventory hash and the existing original input
inventory must agree. The manifest has no source-commit field to invent: original
source remains bound by the reviewed artifact/runtime-package chain.

These are raw original byte and field checks in addition to source-pinned G's
launch authorization; copied hash fields, live-ready booleans and opaque receipt
claims do not substitute for them. L acquires no filesystem or process authority.
Physical fixed read-only mounting remains G's reviewed acquisition responsibility.
The static expected-anchor set stays exactly 15 keys; no final manifest/spec or
input-inventory hash is introduced into its own prerequisite review.


Manifest parsing is variant-specific: original primary
`recall-gateway-manifest/v1` and settlement `recall-gateway-manifest/v2` use their
exact existing `gateway/policy.py` required fields and optional `output_cap_mode`;
settlement-only fields are not invented in v1. Both have null anchor allocation
and bind the original proposal hash. Original D28
`recall-exclusive-embedding-manifest/v1` uses the exact `cohort_policy.py`
required fields and typed pins, nonnull original allocation, and the existing
anchor proposal fallback to that allocation; it has no new proposal field.
No variant may be translated into another, and unknown/extra fields refuse
according to its original schema. The spec remains `recall-gateway-launch/v1`.
This v2 consumer already requires the original input inventory, so its spec must
contain the corresponding original `inputs`. Legacy G paths outside this v2
carrier retain their existing optional-input schema. No product feature or
original launch mode is removed by this qualification requirement.


The separately owned full-workload snapshot inspector has one fixed stdout
exception, `snapshot-inspection`, with the original process intent and bounds
in `process-stream-role-contract.json`. Its 32 MiB + 1 byte stdout and 1 MiB
stderr reservation are taken once from the existing shared capacity before
spawn. This does not enlarge ordinary process streams or the installed 19-case
lane. The same report bytes must not acquire a second kind in another inventory.
An explicit reservation callback is a producer seam; the composed owner must
supply the actual shared capacity implementation. A callback-presence flag does
not independently establish reservation accounting.
