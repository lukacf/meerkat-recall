# Installed offline FastEmbed observation

This finite recipe installs one independently verified Linux archive built with
default features plus `rocksdb,fastembed`. It uses the existing installed driver
process owner, HTTP/MCP capture, installer, original process journal and immutable
semantic corpus/oracle. It never downloads a model, generates an answer, selects
ambient credentials, changes an existing space or rewrites a query.

The source plan is `local_fastembed_plan.json`: 28 original revisions in their
original order and 24 original questions on each public transport. The fixture
uses two explicitly authorized fresh spaces, `semantics-household` and
`semantics-business`. The source acceptance route is `/v1/sources/ingest` with
`ingest_mode: "retrieval"`, stable original external identity, immutable document
identity, explicit predecessor generation and original operation completion.
HomeCore guidance stays in `doc.metadata.purpose`. It verifies immutable rendered
revisions against the existing independent rendered map, including root LF and
original paragraph/chunk spans, both actual embedding markers and complete public
coverage before querying. Search uses `ranking_mode: "semantic"`, HTTP scoped
search and MCP `search_scoped`, limit three and the original revision constraints.
The root node's API role is `document` (`NodeRole::Document`); `root_span` is only
the frozen rendered map's coordinate key. A wire role named `root`, missing or
duplicate document roles, or shortened coverage is rejected.

The model is `bge-small-en-v1.5`, 384 dimensions, `text-v1`, manifest
`0e4372cdf7ebeabea95aa136b56d1d8856c3cf3a72e5224d7b00b05245ad4442`.
The existing model4 candidate root is mounted read-only at `/model-assets`;
`file:///model-assets` is the parent endpoint. Preserve all six exact files in
the source plan. Do not provision again or point at the version directory.
The product uses statically linked ORT `2.0.0-rc.12` API24, not the Python ORT1.20
used for model conversion. The operator supplies an independently reviewed
actual final build receipt binding original feature fingerprint, static link
output and distribution checksum to this exact source/archive/binary. There is
no shared-library mount, `ORT_DYLIB_PATH` or runtime download. The child receives
only explicit bounded thread/offline settings.

Runtime readiness calls actually validate both embedding operation assets. In
Retrieval-only mode the aggregate Graph status may be HTTP503 and `ready:false`:
the observation retains this response and requires storage readiness, approved
and available query/document embedding operations and their exact partition
recipes. It never asserts Graph readiness or canonical-answer groundedness.

The fixed original allocation is 1800 seconds including 90 seconds of cleanup
reserve, 1200 seconds for all publications, 600 publication polls at at least two
seconds within each pending operation, 1024 total HTTP attempts, 16 installed
process invocations, 1MiB per process stream, 64MiB total captures and 256KiB per
HTTP response. Source replacement polls are global, not renewed per revision.
There are 56 required paragraph/chunk embedding effects and 48 original search
requests. These are **not physical model-call counts**: public routes do not
expose physical local attempt identities. This field remains unmet. No harness
retry is made after a failed source or query. The outer operator additionally
owns CPU/RSS/address-space/stage/disk-ledger enforcement and namespace custody.

An original operation's `failed_retryable` receipt remains under observation:
the product scheduler owns its bounded retry policy. The driver polls the same
accepted operation without resubmitting, changing its identity or renewing any
deadline or global poll allowance. `failed_final`, changed source authority,
poll exhaustion or deadline expiry still ends publication unsuccessfully.
Completion still requires the exact published revision, clear error and matching
source head before immutable evidence is checked. Closed-world multi-poll tests
exercise this publication loop; they cannot establish product retry success or
installed model behavior. Earlier observations that stopped on a retryable
receipt remain failed observations, with retry exhaustion undetermined.

The whole clock starts in the outer controller before setup, using the original
`mono_before_ns`, `wall_ns`, `mono_after_ns` sample. The driver inherits that sample
and absolute expiry, stops work 90 seconds before its original deadline and
reserves the outer final30 seconds during terminal model hashing. Original
process cleanup and capture are attempted even if model-cache verification fails.
The namespace must be distinct from the parent, initially contain only loopback
and have no external routes. A host namespace/AppArmor failure is a prerequisite
failure; there is no host-network fallback. Actual mountinfo, process custody,
raw original HTTP responses, source operations, configurations, per-transport
scores and model cache before/after receipts are retained outside the model root.

An inert plan can be obtained after source review:

```sh
python3 -I -B scripts/qualification/local_fastembed_driver.py plan
```

Execution requires a separately reviewed outer controller allocation. Its guest
entry is:

```sh
python3 -I -B /reviewed/scripts/qualification/local_fastembed_driver.py run \
  --manifest /inputs/local-allocation.json --sha256 ROOT_ALLOCATED_MANIFEST_SHA256 \
  --parent-network-namespace 'net:[ORIGINAL_PARENT]' --execute-reviewed
```

The allocation object must contain the following complete fields, with actual
operator values replacing placeholders; placeholders never authorize execution:

```json
{
  "format": "recall-installed-local-run/v1",
  "execution_authorized": false,
  "backend": "surreal-db",
  "fresh_spaces_authorized": ["semantics-household", "semantics-business"],
  "source_commit": "ACTUAL_FINAL_40_HEX_SOURCE",
  "archive": "/inputs/recall-linux.tar.gz",
  "archive_sha256": "ACTUAL_ARCHIVE_64_HEX_DIGEST",
  "binary_sha256": "ACTUAL_INSTALLED_BINARY_64_HEX_DIGEST",
  "run_directory": "/qualification/run",
  "expires_at": "ACTUAL_ORIGINAL_UTC_EXPIRY",
  "original_allocation_clock": {"mono_before_ns": 0, "wall_ns": 0, "mono_after_ns": 0},
  "expectation_review": "/inputs/expectation-review.json",
  "expectation_review_sha256": "ACTUAL_D_REVIEW_64_HEX_DIGEST",
  "nonce": "ACTUAL_FRESH_16_TO_64_ASCII_NONCE",
  "fixture_timestamp": "2026-10-07T00:00:00Z",
  "model_asset_root": "/model-assets",
  "model_asset_manifest_sha256": "0e4372cdf7ebeabea95aa136b56d1d8856c3cf3a72e5224d7b00b05245ad4442",
  "runtime_binding_review": "/inputs/static-runtime-review.json",
  "runtime_binding_review_sha256": "ACTUAL_NATIVE_RUNTIME_REVIEW_64_HEX_DIGEST"
}
```

The static runtime receipt has format `recall-installed-local-static-runtime/v1`,
`reviewer:"lifecycle"`, `reviewed:true`, `actual_build:true`, `link_mode:"static"`,
`ort_version:"2.0.0-rc.12"`, `api:24`, `load_dynamic:false`, and the exact final
`source_commit`, `archive_sha256`, `binary_sha256`. It carries original
`ort_features` including `api-24` and `download-binaries`, excluding `load-dynamic`,
`static_link_directive:"cargo:rustc-link-lib=static=onnxruntime"`, and the actual
`feature_fingerprint_sha256`, `link_output_sha256`, `distribution_sha256`. The
receipt also names `original_build_receipt:{path,sha256}`, the two original
`feature_receipts:[{path,sha256},...]` for ort and ort-sys, `link_output:{path,sha256}`,
`distribution:{url,sha256}` and actual `static_library:{sha256,bytes}`. Features
must exclude `disable-linking` too. The artifact owner must retain the underlying original build evidence and review its
relationship to the binary; digest-shaped invented strings and old host build
receipts are not final artifact evidence. This attestation is captured read-only,
never manufactured by the driver.

The independent review has format `recall-installed-local-review/v1`, reviewer
`qualification (independent D lane)`, `approved:true`, and `plan` equal to the
complete original plan output. Its hashes cover the exact driver, contract,
existing lifecycle helpers, corpus, approval, rendered map and independent
oracles. The outer owner must bind those bytes before invoking Python; the driver
also rechecks/captures original inputs before any installed process starts.

Qualification fields stay separate. Original immutable quote/revision checks,
all required support in first three evidence ranks, denominator three and
macroMRR at least0.90 come from the unchanged independent oracle. Seeded384
offline fake-vector controls17,219,20261006 must reject shape-valid meaningless
vectors. They do not qualify an installed fake-vector semantic index: that
separate actual control remains explicitly unmet. Canonical groundedness needs
independent response-bound review and is unqualified here. Every observation has
`product_acceptance:false`; an exit0 is a complete bounded observation, never an
independent final product verdict.

`test_local_fastembed_driver.py` contains closed-world contract and cleanup
controls. It launches no subprocess, service, listener, inference or provider.
Run it only in an allocated helper-test lane after coherent source review.


### Source-head reads in local publication

`LocalDriver.publish` reads `GET /v1/spaces/{space}/sources/{external_id}/head`
before a replacement and after its original operation completes. The shared
`source_path` helper returns the mutation URL, so the publication reader appends
`/head`; it does not change the shared PUT/POST path or retry a 405 response.
Both path segments remain individually percent-encoded.

The four `LocalContracts.test_native_*head*` publication controls simulate only
the HTTP peer and run the actual source-request, acceptance and original-operation
identity checks. They cover the first publication, replacement, changed or
unpublished heads, and refusal of a changed predecessor before ingest. They stop
before evidence reads and cannot establish installed inference or query success.
The native one-document/two-query observation still requires the original head,
document, source, pipeline, nodes, immutable revision and exact query evidence.

The full local runtime receipt retains the local static-runtime schema and may
record either the lifecycle reviewer or the actual `gcp-recall-codex` reviewer.
Both require the same final artifact identity, actual build, API24 static-link
and feature evidence. A Root review is recorded under its own identity; it is
not attributed to a prior lifecycle reviewer.
