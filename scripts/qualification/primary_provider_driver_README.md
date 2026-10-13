# Primary real-provider capture driver

`primary_provider_driver.py` implements the ordered public workload from the
frozen primary corpus. Running it without `--execute-authorized` prints a source
inventory only. It neither starts Recall/SurrealDB nor creates configuration,
grants, credentials, model assets or provider approvals. No execution, provider
spending or qualification is authorized by this source checkpoint.

The installed invocation is once per persistent backend, inside C/G's same
isolated runtime, after explicit migration, classified named-space bootstrap and
configuration readback. The outer G allocation owns the combined 432 physical
dispatches, $75 cap, 90-minute deadline and terminal settlement. The original
proposal remains inactive. This driver has a 45-minute maximum per backend,
15-minute publication phase, 15-second ordinary/query request deadline, bounded
conditional calls, 8192 public requests and 1 GiB private captures. These driver
limits do not replace the physical gateway's limits.

The runtime must preserve the source-relative paths listed by `inventory()`.
The four primary driver modules use Python 3.11 stdlib and the existing reviewed
`recall_workload.read_frame`/`endpoint` transport. That helper imports
`installed_linux_offline`, `recall_preview`, D's `fixtures`/`matrix`/`oracle`, and
`scripts/release/artifact.py`; the complete dependency digests are part of the
driver review. No ambient `PYTHONPATH`, SDK package or provider key is required.

## Ordered behavior

1. Validate the separate run approval, D's expectation seal, exact corpus digest,
   source/artifact/runtime/configuration identities, C's installed clean-store
   receipt, G's launch context and both explicitly supplied synthetic token files.
   Compare actual named-space configurations with their reviewed snapshots.
2. Read the complete visible source inventory and require the clean-store start.
   Submit each of the 28 original source revisions through `/v1/sources/ingest`
   using the retained typed Source+Doc Graph route. Preserve the original text,
   distinct policy levels, immutable request time and idempotency key. Replacements
   compare the actual original completed head generation. Submit each request
   once. Poll only its original durable operation. A failed predecessor blocks
   later revisions of that source; it never triggers a replacement request.
3. Require original operation completion and current head identity. Capture the
   actual source, immutable document, node inventory and pipeline status through
   public reads. Verify exact independently mapped rendered text, UTF-8 spans,
   profile markers and complete embedding coverage. Keep raw Source hashes tied
   to submitted input; rendered Doc bytes are checked against D's separate map. Capture each revision before advancing to its successor.
4. Run D's separately frozen conditional cases through the stateless maintenance
   MCP route. Supported actions are actual `create_reprocess_job` with only
   `re-maintain` and polling its original job, scoped `compile_wiki_article` after
   observed-ID Definition preparation, and the separately retained
   `create_wiki_from_graph` contract with an explicit bounded article budget and
   `manage=false`. Capture actual articles or reconciliation from the selected
   contract, never fixture-supplied winners.
   Every omitted category needs D's pre-output `not-applicable` reason.
5. Submit all 24 original queries verbatim in semantic and hybrid modes, separately,
   in original order. Use limit 3, explicit space and the whole current inventory
   as readiness constraints. There is no relevant-source filter, `use_vector`
   inference, query rewrite, warmup, rescue search or expected-document fetch in
   the query phase. Keep failed responses in the original 48-case sequence.
6. Save exact request intent before sending, then the complete bounded raw response
   (or its explicitly incomplete prefix), framing result, elapsed time, hashes and
   error type. A crash leaves an unsettled original request file. A new output
   directory is mandatory; the driver has no resume/retry mode.

`capture_complete=true` means the scheduled public observations completed; it is
not a semantic, artifact, account-billing or provider verdict. Every output sets
`qualified=false` and `product_acceptance=false`. D must independently review the
unchanged wire evidence and join actual G terminal-settlement receipts, capture
inventory, isolation and installed artifact evidence. Semantic and hybrid files
are explicitly `real-provider-unqualified` until that external custody review.

## Review and launch seams

The driver manifest format is `recall-primary-provider-run/v2`. It supplies
`execution_authorized`, `provider_dispatch_authorized`, `authentication_enabled`,
`backend`, `endpoint`, `nonce`, `expires_at`, `fixture_timestamp`, exact
`source_commit`, `artifact_sha256`, `runtime_sha256`, `configuration_sha256`,
`output_directory`, and `token_files` keyed by the two exact spaces. Token files
must be signed synthetic API tokens in private regular `/work` files; the driver
does not mint or import a host signing key.

`inputs` maps `corpus`, `expectations`, `proposal`, `configurations`,
`conditional_cases`, `bootstrap_plan`, `rendered_map`, and `review` to immutable
`/inputs` paths plus raw-file `sha256`. The independent review binds exact raw
plan bytes, with no plan-to-review projection or future output hash. C's static
plan has no review/template/configuration hash. Its reviewed phase template
joins actual dynamic `observations.runtime_receipt` and `gateway_context` only
after startup. The complete source map includes all four B modules, C's producer,
contract and IO modules, G's validator, shared helpers and D's rendered map.

D's exact `primary-rendered-map.json` raw hash is
`aa3579d3252fa6cbe396a1003d84124b1c9541857fa78176eec57317045fb7d1`.
It binds all 28 original revisions to exact rendered text/hash and
root/paragraph/chunk spans. Missing, foreign, changed or truncated expectations
refuse. Observed text is never trimmed or normalized to make it match. Original
corpus/query/allocation and source request bytes remain unchanged.
The root span must be covered by exactly one public node with role `document`,
the serialized `NodeRole::Document` value. The map's `root_span` key is retained;
literal role `root`, other role aliases, missing/duplicate document nodes and
changed document coverage refuse.

Scoped primary wiki cases use the existing `compile_wiki_article` tool. Their
frozen Definition selects the original household dishwasher or business Lantern
subject with the exact named-space classification, scan limit 256 and membership
limit 64. The collector calls public Definition creation, reads the same returned
ID, and requests its current context before compiling that same ID once. Caller
IDs, alternate selectors, retries and model overrides are refused. The original
whole-graph wiki case remains a separate supported contract.

The Definition must be explicitly published. Its deterministic Query projection
is an inspector record with publication status `draft`; this is the actual public
wire contract. That context must be current, complete, nonempty and untruncated.
The resulting Article must be newly compiled, published on the wiki surface,
and carry clean validation and freshness receipts. The collector checks original
Definition identity, complete unchanged subjects, versioned context dependencies,
original current Source IDs, immutable revision spans and exact UTF-8 quote hashes,
and evidence-entailment statements whose Markdown matches their statement ledger.
The complete rendered article must also match the original compiler's title,
headings and checked block bodies, including separators. Extra prose outside the
statement blocks is refused. This mechanical check does not grade titles,
headings or claims; independent semantic review still covers the complete article.
Versionless dependencies can gain an observation while the compiler adds its
source catalog; the collector does not invent a public epoch number. The original
compiler's transaction fence remains responsible for atomic publication.

The reviewed bootstrap plan must carry `scoped_wiki: true` exactly when these
scoped cases are present. Producer and independent startup validation separately
bind its optional `write:entity` and `manage:wiki` grants before startup; the driver
rechecks the case/plan binding before reading credentials or issuing requests.
All initialization, Definition preparation, compilation or maintenance-job polling
uses one original 720-second case deadline, also bounded by the original backend
deadline. Creating a job never starts another polling interval.

Scoped article receipts explicitly report full-graph planner and organizer roles
as `unexercised`. Provider-role dispatch proof still needs original gateway
accounting, and factual quality still needs independent review of the actual prose
and source evidence. A complete public receipt is not semantic or product
qualification. The original corpus, four-case bound, 12 wiki dispatch stop limit,
432 global dispatch stop limit, dollar cap and whole-run clock remain unchanged.

Startup validation ties the declared schema operation to the hash-bound original
command journal and completed command logs, or to the exact successful original
operator `initialize_runtime` wire request/reply (including complete MCP SSE).
It also checks each live service's original command, PID ancestry and executable,
per-space denial/readback captures, original launch anchor and absolute deadline.
A claimed success, suppressed error or truncated response cannot supply proof.

The configuration input maps each space to its reviewed `create_request`,
`source_security`, original `guidance` and `embedding_partition_model`. Actual
created configuration records are attached from the validated startup receipt.
The collector additionally reads the exact immutable revision via
`GET /v1/docs/:doc_id/revisions/:revision_id`, using the original published
pipeline revision identity. Its actual full DocRevision must match the Doc,
space, security, active status, renderer identity, exact text and text hash.
This consumer depends on C's authorized published-revision read endpoint; no
revision body is inferred from the pipeline ID. It adds 28 read-only requests per
backend within the existing 8192-request/capture/deadline budgets. It adds no
provider calls and does not alter the 28 submissions or 48 measured queries.
The private finalizer receipt remains explicitly unmet through public reads.
Every output remains unqualified pending independent complete evidence review.

The bounded conditional format `recall-primary-conditional-cases/v1` contains
reviewer/status, both category dispositions, and up to four named cases with exact
space/tool/arguments. Arguments cannot introduce model overrides or recurring wiki
work. Its representation and exact case selection require D's pre-run review;
this source does not manufacture the approval.

**Outstanding integration contract:** C/G must produce the reviewed
`runtime_receipt` and `gateway_context` consumed by this driver. The latter is a
sanitized launch-context projection, not a replacement ledger or a self-issued
authorization. It binds actual `accounting_policy=full-context-terminal-v2`,
backend, run nonce, runtime identity, verified isolation and the actual 432/$75
physical allocation. C/G must freeze its custody and exact producer before this
driver is executable. Final ledger settlement occurs outside this child and is
never inferred from that pre-run context. No ready-to-spend manifest is included.

Public operation/head reads expose canonical completion but not the complete
private finalizer receipt or immutable manifest. Public search does not expose
request-to-physical-attempt correlation. The observation records both limits as
`unmet`; no timestamp/hash guess is promoted to physical provider attribution.
Model output/evidence judgments remain exclusively D's independent review.

## Offline controls

`primary_provider_driver_test.py` uses explicit in-process fake public responses,
never sockets, services, model SDKs or live providers. It checks the original
28/48 schedule, replacement generation, one-shot failures, exact query sequence,
whole-inventory constraints, original receipt substitution, framing and MCP stream
identity. These controls are not product/provider execution evidence.

Run in the integrated source tree with:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/qualification/primary_provider_driver_test.py
```

For an isolated source owner worktree, `RECALL_PRIMARY_SOURCE_ROOT` may name the
read-only integrated tree containing D's unchanged frozen JSON inputs. Test
temporary captures stay inside the owner worktree and are deleted on completion.

The inert inventory supports the installed `python3 -I` invocation. Sibling
modules resolve from the reviewed script location, independently of the current
directory and `PYTHONPATH`. An offline regression launches only this inert Python
entry point from a temporary directory containing a hostile same-name module;
no service, credentials, SDK or network request is involved.

Startup observations and the original clock
------------------------------------------

Immutable reviewed inputs are the corpus, expectations, inactive proposal,
configuration creation requests, conditional cases, rendered map and bootstrap
plan. The review seals exact raw bootstrap plan bytes. No review reference or
future configuration/template hash appears inside that plan. The final manifest
records the same exact complete plan bytes and a separate review reference. Static configurations contain `create_request`
with exact POST fields; generated IDs and timestamps come from actual startup
readbacks. The collector uses those original readbacks for subsequent comparison.

The final manifest has a separate `observations` map for `runtime_receipt` and
`gateway_context`, each containing the actual private path and hash captured after
startup. D's prior review binds `observation_contracts` and all immutable
`expected_launch` identities; it cannot prehash future evidence. The collector
calls C's reviewed pure validators and G's actual `read_context` validator,
verifies raw request/response custody and per-space authorization denials, and
requires the original live bridge -> bootstrap -> collector/service ancestry.
Missing proof refuses execution. C's narrow bootstrap receipt remains
`qualified=false` and `product_acceptance=false`.

The backend allocation begins before startup. The collector consumes its exact
`original_deadline_monotonic_ns` and checks it against G's original whole-run
clock; it never grants another 2700 seconds at collection time. Dynamic receipt
hashes and original startup clocks are retained in the observation. These source
changes do not authorize a run or cure a blocked namespace prerequisite.

Static plans and their prior review exclude the later outer launch-spec and
approval-manifest hashes. G binds those in the original read-only launch anchor,
including the actual immutable `/inputs` inventory. The collector validates that
anchor through G and compares the resulting launch-spec hash with the dynamic
receipt/final manifest. Runtime packaging precedes the static input tree; no
inner input contains its own enclosing tree or runtime hash.

Source followup validation covers the final C acyclic raw-plan contract, exact D
rendered map and actual captured startup execution evidence. Offline controls use
explicit fakes and read-only independent expectation files; they do not exercise
Recall services, sockets, provider SDKs, or Rust binaries.


The v2 entry point requires a separate, original D runtime review at
`inputs.runtime_review` and a C sidecar at `observations.runtime_custody` before
any corpus acceptance or credential loading. The primary driver review format
is `recall-primary-driver-review/v2`; the original C bootstrap and G context
remain v1, with their original bytes and narrower claims. Bootstrap-only v1 run
manifests are refused by this entry point.

The sidecar is exactly `{format, observed_runtime, controller_context, files}`,
with format `recall-runtime-custody/v2`. `observed_runtime` is D's complete
`recall-dynamic-runtime-evidence/v2` envelope. Each file entry is
`{path, ref: {sha256, bytes, kind}}` and points to original retained `/work`,
immutable `/inputs`, or the original `/launch-anchor.json`. On disk,
`controller_context` has exactly `ref`, `observed_wall` and
`observed_monotonic_ns`; the adapter restores the exact original context bytes
before calling L's pure validator. It never regenerates the context or installed
prerequisite JSON. The additional immutable `inputs.runtime_original_inputs`
contains `{format, review, installed_offline, files}` with format
`recall-runtime-original-inputs/v2`. Its `/inputs` references must appear unchanged
in the sidecar, and its review must be the same original `inputs.runtime_review`.
This location bundle is bound by the original G input inventory; it does not
create a future-receipt hash dependency in the prior D review.

The adapter checks the complete advertised inventory before reading, rejects
duplicate paths and conflicting reference types, and bounds the advertised
inventory to 352 MiB, 64 MiB per file and 16,384 entries. A caller-owned lazy
Mapping reads only requested original records and verifies their hashes and
lengths on every lookup. It has no body cache and never materializes the full
inventory. The shared validator must use that Mapping without converting it to
an eager dictionary.
These are reader bounds on original evidence, not additional capture or provider
allocations. C's sidecar is capped at 8 MiB. L's reviewed
`validate_runtime` is the sole installed-runtime predicate. Missing installed
19-case evidence, changed source/helper/runtime identities, or any predicate
failure prevents collection. Reading and validation consume the original backend
deadline, and the original live process ancestry is checked again afterward.
The resulting scoped component, including outstanding owned-service
cleanup obligations and proof limits, is retained in the primary observation;
it does not qualify provider settlement, semantics or the product.

`primary_provider_driver_runtime_test.py` tests only bounded original-byte
loading, lazy lookup, changed-file refusal and validator invocation with an
explicit in-memory spy. It is not an
installed-runtime oracle or a replacement for the independent validator tests.
The v2 path remains held until the C sidecar producer, L validator and D exact
source review are composed. No ready-to-execute manifest is supplied.

The isolated primary executable pins both the dynamic validator and its actual
installed-evidence helper, plus C's custody producer, in its source inventory.
The canonical installed-validator import is bound only around the pure runtime
predicate and restored on success or refusal. It does not extend Python import
search or use a caller's cached validator. This import control does not establish
that the original installed/runtime evidence passes either predicate.

The v2 runtime component must preserve the original run's paired executable
identity as well as Recall's executable identity. The legacy field
`paired_artifact_sha256` hashes `/opt/recall/bin/recall-surreal`; it does not
establish the paired release archive's provenance. Recall's v2 component archive
field remains separate from its `binary_sha256`, which maps to the primary run's
legacy `artifact_sha256`. A component for another paired executable is refused
before credentials or workload dispatch.
The primary source inventory also pins the actual shared
`installed_process_capture.py` required by C's bootstrap loader. Its complete
required helper set must fit the exact primary review map; adding process capture
cannot create a plan that startup requires but the primary collector refuses.

The primary helper loader hashes one original source read and compiles those
same bytes. Loading C/G/M/L cannot execute a later filesystem read or unrelated
cached bytecode after the reviewed digest check. The packaged runtime must still
be immutable and all helper identities remain independently reviewed.
Compilation also disables inherited future flags, preserving the original
module's own annotation semantics instead of the loader's compiler context.

The primary v2 evidence reader also admits exactly the original gateway mounts
`/gateway-manifest.json` (`gateway-manifest`) and `/gateway-launch.json`
(`gateway-launch-spec`). Each is bounded to 131,072 bytes before any read and
must remain a private regular file owned by the current user. No-follow open
and before/after file identity checks reject symlinks, growth and same-length
mutation. The lazy mapping still rereads and rehashes every original path and
same-digest alias on each lookup; it does not reconstruct missing mounts from
static inputs. These bytes consume the existing 352 MiB aggregate allocation.
Other root paths remain forbidden, and the immutable input-only bundle remains
restricted to `/inputs`. The shared L predicate retains all outer-launch
crossbindings and remains the sole runtime acceptance authority.
