# D28 bounded workload driver source

This is a read-only collector for a separately supplied qualified installed
artifact and prepared corpus. It does not ingest, mutate, build, start services,
download models, discover credentials, retry, execute fallback, or directly call
a provider. It has not been run against an endpoint. Source/mock passes are not
product or latency acceptance.

Root froze **50 total source identities across two spaces**, **50 callers with
four sequential original requests each**, and **200 measurement requests per
backend**. The limit is 50 results with complete quotes and 262144 JSON bytes.
Each request has an absolute 8-second deadline; the complete cohort has a
120-second window. D owns strict p95 < 2000 ms and p99 < 5000 ms expectations.
Actual server/request overlap is unknown until measured. OB3's 140 runs over
51 minutes, observed peak 50 running analysis workers, and mean about 34 workers
do not establish Recall request concurrency.

D owns a **separate** frozen workload corpus, original query/evidence cases,
schedule and evaluator; G owns the separate provider accounting authority. The original semantic corpus and
thresholds are unchanged. The driver validates 50 unique `(space, source key)`
identities in the workload corpus and 200 explicit unique `(worker, ordinal)`
requests. No lowered `limit`, rewritten query or lexical substitute is accepted.
Revision, retraction, permission and outage scenarios have separately declared
prepared states and explicit request/provider budgets outside the 200-request
measurement. This driver performs no automatic state transitions or extra phases.

The D adapter is `tests/recall_preview/workload.py`. Its actual public source
interfaces are `request(query)` for the original wire body,
`workload_response(query, response, corpus)` for an independent response verdict,
and `evaluate_cohort(observation)` for complete-cohort evaluation. Its v2 contract
and corpus schedule are consumed unchanged, including caller IDs, ordinals,
barrier timing, top-three evidence metric, all-50-result validation and MRR.

G owns a separately pinned bridge under `tests/recall_preview_semantics/gateway/`.
It must expose the two root-named provider-accounting interfaces; these are not
methods of D's evaluator:

- `authorize_provider_allocation(manifest, contract, allocation)` must durably
  admit the exact exclusive cohort **before any send**, return a bounded JSON
  begin receipt, and raise on refusal. The collector validates its exact backend,
  measurement phase, allocation hash, original cohort and baseline identities. Local credentials and input checks finish
  before this call. Its receipt is `observation.provider_cohort_begin`.
- `provider_accounting(manifest, observation, allocation)` must close admission,
  settle or retain liability for original active sends, and seal the exact
  physical-attempt set after request tasks finish. Begin is bounded to 10 seconds
  and terminal accounting to 130 seconds, outside the 120-second measurement
  window; original G cohort deadlines remain authoritative. It runs on collection failure,
  cancellation and refused/partial begin as well as success. It returns a JSON
  object with `verdict` (`passed`, `failed`, or `unmet`). A passed close must match
  the original cohort/baseline, close admission, have zero unsettled sends and
  retain sealed capture identities and bounded physical counts. It cannot claim
  per-request association.

The collector consumes G's concrete `gateway/cohort_bridge.py` API; exact source
still requires D's independent review. No G authority is implemented here. The manifest and D review must both pin the bridge hash. Missing source or
interfaces refuse before a socket opens. D's frozen three evaluator methods
remain unchanged. A permission flag alone cannot prove the gateway enforces the
allocation. Exact final driver, bridge, dependency, case and schema hashes are
prerequisites and must be reconciled before an execution review is sealed.
Provider attempt IDs are not public search DTO fields. The driver leaves each
row's `provider_attempt_ids` null unless an independently validated association
exists; it never fabricates an association from timing, query similarity or
caller count. An exclusive cohort establishes total physical counts and charges,
while per-request attribution remains explicitly unmet. The independent D
performance verdict is preserved even when accounting fails; `collector_verdict`
passes only when both independently pass. Missing accounting fails the packet. The earlier
432-request semantic allocation does not authorize this workload.

`inventory` is inert. `run` additionally requires an expiring explicit execution
and provider-dispatch allocation, exact artifact/source/binary identity, D review,
contract, corpus, prepared-fixture and original provider-allocation hashes.
`provider_phase: measurement` and a fresh bounded nonce are explicit. Both D's
`workload-preparation-contract.json` and `workload-allocation-typed-delta.json` are
separately hash-pinned in the manifest and D review, and the independently
qualified prepared-fixture receipt must bind those identities, original
allocation and exact backend. A receipt from another backend or the earlier
normalized-input proposal cannot qualify this run. A literal
loopback HTTP endpoint is required; no DNS, proxy, redirect, or remote URL is
followed. The supplied candidate may itself require separately authorized
governed providers, which is why the accounting check is mandatory. Credentials
come only from explicitly named private files. No ambient token is inherited.

The collector uses bounded asynchronous HTTP/1 framing and D's actual strict
JSON parser. D's SIGALRM context manager is intentionally not used in concurrent
workers because it only supports one main-thread deadline. `asyncio.timeout`
covers connection, write, headers and body, including trickling responses.
Duplicate/ambiguous framing, invalid chunk termination, missing final chunks,
oversized bodies, malformed JSON, duplicate keys and nonfinite numbers fail.
Private captures retain the body prefix and distinguish framing completeness
from JSON/correctness. Header and wire caps are 32768 and 2097152 bytes; aggregate
private capture files are capped at 128 MiB. At most 50 callers are active. An
expired cohort cancels remaining calls and retains canceled attempts; it does
not start another request after cancellation.

Each row captures caller start, connection completion, client write completion,
first observed response byte, response completion, status, complete-body framing,
bytes, original request hash and independent verdict. Client lifetime overlap and
write-complete-to-response-complete overlap are reported separately. These are
not certificates of simultaneous server execution. All failed and timed-out
requests remain in the sample; no successful-only percentile or hidden retry
can qualify. OB3's operational smaller-payload retry/fallback is a separate
consumer behavior and is not used to rescue the fixed-limit stress run.

The prepared-fixture source counts are explicitly attributed to the reviewed
preparation receipt and frozen corpus. The collector does not invent a live
inventory or implicitly perform extra preflight requests. The exact endpoint's
artifact identity and preparation must already have independent qualification.
The receipt always has `product_acceptance: false`; D supplies the bounded
independent workload verdict.

Source inventory and offline mocked tests (no sockets, services or providers):

```sh
python3 scripts/qualification/recall_workload.py inventory
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s tests/recall_preview_semantics -p workload_driver_test.py -v
```

Future execution requires a separately completed manifest based on
`workload_driver_manifest.example.json`, all D hashes/interfaces and explicit
root allocation. No placeholder review/preparation/provider fields authorize it:

```sh
python3 scripts/qualification/recall_workload.py run \
  --manifest /absolute/allocated-workload.json --execute-reviewed
```

Passed cohort accounting must reserve exactly 1,064,960 nano-USD for every
physical dispatch reservation, without refunds, as frozen by D/G. Integer fields
cannot be booleans. Offline stalled-close controls exercise an actual absolute
POSIX deadline around a mocked blocking bridge, preserving unmet accounting
and the original independent verdict without opening a socket.

The reusable asynchronous frame parser accepts an explicit keyword-only
`body_limit` from 1 through 270336 bytes for D's separately reviewed container
MCP envelope capture. Its default remains 262144; the D28 workload always uses
that default and its frozen response budget is unchanged. Content-Length,
chunked and EOF framing enforce the same selected bound, with strict integer
validation before reads. This parsing seam grants no execution or acceptance
authority; callers retain their absolute deadline and exact pinned contracts.
## Exact rendered preparation identity

Both phases now require D's separately reviewed `workload-rendered-map.json` as
an immutable static input. The manifest and D review pin `rendered_map_path` and
`rendered_map_sha256`; the independently qualified prepared-fixture receipt must
bind the same digest. The original corpus, queries, typed contract and allocation
remain unchanged. The shared validator rejects missing/foreign/duplicate rows,
changed raw/rendered hashes, UTF-8 lengths, source identities or default-tree spans.
Actual published documents include the renderer's one LF; paragraph/chunk spans
exclude it. No candidate output is normalized to fit the expectation. The pinned
map describes expected rendering, not an independently qualified runtime or a
replacement for actual preparation/index evidence. This additive static identity
grants no permission, calls, spend or elapsed time.
