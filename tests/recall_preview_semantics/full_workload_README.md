# Full workload source implementation

The independent contract is `tests/recall_preview/full-workload-contract.json`
from qualification commit `6d3127e00667832d8c0963ce44ab7ea473b8c0cf`, byte SHA-256
`44a91b9f0ae4c2ae32d2f8aec9eb783a48ccca63eeee010ff2427ef1ec4a0c0b`.
It specifies fourteen cases, 56 sources per backend, original policy3/7 spaces,
two exact two-million-byte Retrieval inputs, and 140 callers making two lexical
requests each. Existing 50-caller corpus and provider allocations remain unchanged.

The first source component is `full_workload_transport.py`. It accepts only an
explicit IPv4 loopback HTTP endpoint and signed named-space credentials, bounds
requests, response framing and captures, and observes the original absolute run
and per-request deadlines. Every physical public request consumes one count;
there are no implicit retries, redirects or proxy discovery. Original bodies,
raw response prefixes and complete/failed framing observations are written to a
new private directory. Credential values are not recorded; their hashes and
explicit roles identify the selected principal. Actual request intervals cannot
establish server execution concurrency or a query-to-provider association.

`full_workload_test_transport.py` uses memory streams and mocked connection
creation. These controls exercise parsing, cancellation and capture behavior,
not an installed service, provider, kernel namespace or real network scheduling.

The scenario driver and owned-spy adapter are under implementation. This
component checkpoint cannot run or qualify the full workload. The independent
mutation/structured-output recipe supplement is required before those phases
can accept fixtures. Current runtime namespace availability is externally
blocked; no fallback or live execution is authorized by these files. Controlled
spies qualify protocol and ordering only. Semantic 140-caller qualification
remains required and inactive pending its own reviewed allocation and thresholds.
FW13/FW14 consumer completion belongs to the consumer implementation; private
manifests, physical vectors, erasure and backup retention are never inferred
from public success or absence.

`full_workload_spy.py` is the controlled provider fixture component. It has no
upstream client or forwarding path. Its optional listener binds only an explicit
IPv4 loopback port; the process/isolation owner supplies the namespace. The
independent `recall-controlled-graph-spy/v1` recipe pins each exact space, model,
credential hash, path, user-text selectors, full output/support expectations,
physical-call limit, response status schedule and held-call ordinals. System
examples cannot select a fixture. Support compares the complete independently
authored semantic tuple and exact quote, digest and typed byte coordinates;
unknown interpretations receive `supported: false`. Fixed vectors establish
embedding protocol plumbing only.

The actual generation and embedding wires have no `X-Space-Id`. Provider
admission requires one declared space for the exact credential digest, model
and path. Embeddings also require one rule; generation retains its original
user-text selectors among multiple rules inside that space. Ambiguous scope is
refused before a listener starts and checked again at admission, even when one
space's source text or an explicit header would otherwise select a rule.
A present conflicting header is refused. Capture preserves the
original header as `wire_space` (including `null`) and labels the derived space
authority explicitly. Text, request order and timing cannot choose a space.
The owned workload supplies distinct fixed fake credentials for its two spaces.
The product's MCP tool-server header is not a provider header. Specialized
spies retain the two existing response/validation hooks and their original wire
metadata when calling the base selector.

Each actual physical connection consumes the original finite cap. The complete
request and its original rule/ordinal are durably captured before a response can
be held. `wait_held(rule_id)` returns that physical call ID; `release(id)` can
release it once. It grants no source authority and does not complete product
work. The original absolute deadline and shared capture budget apply throughout.
Protocol/capture/authorization failures terminate the fixture; a fresh incoming
request cannot reset it. Explicit scheduled HTTP 400/503 responses remain real
captured attempts, with no fixture retries. Owned cancellation and teardown
preserve failed captures and never fabricate product completion.

The offline controls use in-memory HTTP streams, mock writers and private
scratch files; process creation, listeners and connections are forbidden by the
tests. They cover original held intent, 503 count, credential/space isolation,
expiry, cancellation, truncated/ambiguous framing, exhausted capture, support
semantics and both actual streaming response envelopes. These are helper tests,
not installed runtime qualification or semantic quality results. The full
workload still needs the independently frozen literal extraction/support recipes,
actual Graph/public lifecycle driver, and installed service/isolation evidence.
The generic recipe schema is not independent approval of any concrete recipe.

`full_workload_graph.py` exposes `GraphFixture(client, spy, reviewed_recipe,
recipe_sha256)`. The client owns the original namespace, credentials, physical
HTTP count, absolute deadline and capture budget. The recipe pins one complete
typed Graph request, independently expected rendered text/hash, full observable
assertion tuple/quote/span/hash, and the controlled extraction rule. Future
operation, generation and DocRevision IDs come only from actual public responses.
`accept_and_wait()` submits once, polls that exact operation, then verifies its
head, normalized Doc, GraphReady Source, immutable revision mapping, complete
assertion page and actual promoted entities. The separate public
`GET /v1/docs/{id}/revisions/{revision_id}` supplies the full original revision;
the fixture verifies its exact rendered bytes/hash, document and space mapping,
active status, policy envelope and raw-source provenance binding. It retains
the actual DTO and never reconstructs it from the current Doc. This adds one
physical HTTP request under the original caller count, deadline and 3-MiB
capture limit. Larger responses remain unmet even if the server permits them.
It requires a successful completed
physical call to the original rule, so unrelated corpus traffic cannot satisfy
the fixture. Physical vector contents and private publication manifests remain
explicitly unobservable through these routes.

`hold_next_exact_source()` submits the same one original typed request and waits
for the independently scheduled held extraction call. `release_original(held)`
requires that exact accepted-operation descriptor and physical call ID, once.
`wait_complete()` and `observe_completed(operation)` let the owner perform its
reviewed lifecycle action between those phases. There are no implicit submit
retries, terminal-failure retries, generation substitutions or time resets.
`ControlledSpy.handle` can be attached to a listener owned by the consumer's
reviewed isolated topology; the helper creates no container or ambient listener.

The scripted Graph tests validate normalized-byte expectations, generation and
revision identity, full-page coverage, physical-source lineage, promoted policy,
original held-call custody, and failure on unrelated provider activity. They do
not replace real Graph execution. Concrete CB07 oversized extraction, CB09
revocation and full-workload mutation recipes still require independent frozen
inputs and actual installed execution before any qualification verdict.

`full_workload_load.py` implements FW12's controlled lexical scheduling phase.
Its independently pinned plan contains exactly 280 literal requests, one pair
per each of 140 caller IDs, with original named spaces, query bytes and revision
coverage expectations. All 140 callers become ready before the gate opens;
each caller sends its two requests sequentially, once. The original HTTP budget
and absolute deadline apply, with a maximum 180-second load interval and 256-KiB
response bodies. Failed first requests suppress unsent successors while already
owned calls retain their actual captures. Original provider counts must remain
unchanged for this lexical phase.

FW12 always supplies explicit nonempty source-revision constraints. The active
scoped-search implementation seeds coverage from exactly those keys and rejects
other sources before adding coverage. The response predicate therefore requires
the exact expected key set as well as every original row value; an extra key
fails even when all returned evidence belongs to the correct source. Offline
controls retain these coverage-only leaks in the failed response, suppress the
unsent second request, and preserve order independence for JSON object keys.

The observation reports actual request framing/status/coverage, skipped IDs and
client write-complete-to-finally intervals. It makes no server-execution overlap,
semantic latency, model quality or overall product qualification claim. Semantic
load remains inactive pending its separate allocation and threshold review.
Closed-world scheduling controls exercise the actual asynchronous gate with mock
HTTP calls; the 140-caller result from those controls is not installed throughput.

The workload transport also accepts exact `POST /mcp/full` for bounded original
`initialize` and `tools/call` JSON-RPC requests. It sends the fixed MCP protocol
version and JSON/SSE Accept values, retains the exact raw response, and requires
one complete response for the original typed ID. Extra/foreign IDs, duplicate
JSON keys, truncated SSE, unsupported event fields, other MCP paths and non-POST
MCP calls refuse. This uses the same named credential, request count, deadline
and capture budget as ordinary public HTTP; it creates no session, redirect,
header override or independent allowance. Scripted framing controls are not an
actual installed MCP or grant-revocation acceptance result.

The controlled embedding adapter accepts at most 128 explicit nonempty inputs,
matching the source runtime's bounded batch (the OpenAI provider may split that
into 100 and 28). The original 8-MiB response, 4,096 physical calls, 1-GiB total
capture and absolute deadline still apply. Offline controls exercise actual spy
framing and capture for 100 and 128 inputs at 3,072 dimensions and reject 129 or
empty inputs. These fixed vectors demonstrate protocol capacity only; full-input
product preprocessing and actual provider capacity need independent qualification.

FW12 also requires an independently pinned evidence-requirements map for all
280 original request IDs. This is separate from the original load-plan bytes:
each entry has nonempty `required_evidence` and `required_claim_evidence` (which
may be empty), using the driver's literal source/revision/quote/hash contract.
The map's canonical SHA is retained in the observation. Known-positive requests
cannot qualify through coverage alone or omit their expected evidence.

`run_load(..., evidence_requirements=..., evidence_sha256=...,
validate_evidence=...)` invokes the same synchronous `Driver.query_evidence`
callback once per successfully framed, covered response. It passes the original
space, request body, response body, ordinary requirements and claim requirements;
the callback must verify every returned hit, require the original literals, and
return `None` only on success. It uses the driver's already captured source and
immutable revision observations and performs no extra query. An exception or
other return value leaves that captured response with `evidence_verified=false`,
stops unsent second requests and prevents a positive protocol verdict. Callback
validation does not extend measured client wire intervals, reset deadlines, or
change the protocol-only and unqualified semantic-load classification.

`Client.runtime_request(body, response_limit=None)` is the bounded global
inspection path on that same client. It accepts only the inspector's exact
`initialize` envelope or `tools/call` for argument-free `get_runtime_config`,
always at `POST /mcp/full`. The original allocated `runtime:inspect` credential
must exist; the wire omits `X-Space-Id` and captures `space: null`. Named requests
retain their named header and cannot use this global credential. The existing
JSON/SSE parser, matching response ID, protocol headers, request count, capture
capacity, cancellation behavior and original deadlines are shared unchanged.
The inspector owns runtime response validation and the process owner supplies
the explicitly allocated global `read:system` principal; this transport does
not mint a token, infer a grant or create a separate inspection budget. Offline
controls cover both permitted calls, mixed named/global accounting, forbidden
operations, original cap exhaustion and cancellation with token-free captures.

For the native backend's paired service, the process owner calls
`bind_paired_health(reviewed_endpoint)` exactly once before starting its original
child, then `await paired_health(the_same_endpoint)`. The binding opens one
30-second health subwindow within the original run deadline; it cannot be
rebound or extended. Only `GET /health` at that original IPv4 loopback endpoint
is sent, with no authorization or space header and a 4-KiB response limit.
These observations use the same client counters, capture directory and expiry;
they retain the actual endpoint and complete or failed framing. They neither
follow redirects nor establish readiness through status alone. The process
owner must validate its original reviewed endpoint, child identity and actual
response before startup can proceed.

The installed runtime owner uses `owner_request(method, path, body=None,
response_limit=None)` with its separate original `runtime:operator` JWT. Its
fixed allowlist contains only health/readiness reads, bounded snapshot export,
and the owner's exact MCP initialization and runtime setup/worker-activation
patches. Exports require one equal basename/path, 3,600-second retention and
a 1–30-second quiesce timeout. Other tools, routes, body fields and credential
substitutions refuse before sending. The inspector retains its separate
read-only API and token. Both paths share all original accounting and framing.

The existing driver's `NamedClient` remains the sole adapter for `space:role`
JWT selection. The base client retains the exact selected key and captured role;
no new credential mode or second prefix is introduced. Operator, inspector and
paired-health methods use the private shared transport directly, so they bypass
the named adapter while retaining the same counters and captures. Offline
controls exercise that subclass interaction and deny missing named credentials
without fallback. The owner remains responsible for binding runtime paths and
export destinations to its original reviewed plan and for all actual
process/grant authority.
