# D28 public preparation capture

This helper is source-only until D reviews its exact source/cases and G's separate
cohort bridge, and root allocates the installed candidate and provider run. It
does not start a service, configure a provider, download a model or inherit an
ambient credential. It consumes the frozen separate D workload corpus: **50 total
sources, 25 per named space, per backend**. The 200 measured searches remain in
the separate read-only collector; preparation performs no query warmups.

The existing `POST /v1/sources/ingest` receives typed Source and Doc inputs with
`ingest_mode: retrieval`, exact original text, revision and idempotency controls.
The explicit source/document envelopes retain `household-policy` level 3 and
`business-policy` level 7. Normalized ingest defaults to `default`/0 and cannot
prove these distinct record policies. D froze empty subjects, unknown subject state, empty sources/chain provenance,
actor `d28-qualification` and input timestamp `2026-10-07T00:00:00Z` in
`workload-preparation-contract.json`. Stable rendition keys are
`d28/text/{key}/{revision}`. Both that contract and its explicit allocation typed
delta are separately hash-pinned alongside the original unchanged allocation.
Exact configurations and helper source still require independent D review.
Fresh input IDs are deterministic UUIDv5 values bound to nonce/backend/space/key/
revision/type and are input placeholders only. The external-source acceptance
may allocate a different Source ID, and Doc identity is acceptance-bound. Both
actual returned identities are captured and compared to the original operation,
head, source and document; private identity algorithms are never duplicated.

Before any submission, both stored space configurations must match independently
reviewed bytes and all 50 external heads must return typed `not_found`. Each
source is submitted once. A timeout or uncertain POST response fails the capture
without resubmission. Polling uses the original returned operation ID and rejects
changed source/generation/revision, terminal failure, retraction or supersession.
Only original complete operations permit public source/doc/chunk/pipeline reads
and the exact `GET /v1/docs/{id}/revisions/{revision_id}` inspection.
Those reads must agree on exact original source bytes, independently mapped
rendered document bytes, immutable revision references, complete short-source
chunk spans, distinct record policy, unchanged unknown subject
state, actual source/revision node lineage and reviewed embedding profile markers.
The full immutable revision DTO must match the original pipeline revision and
document, classified envelope, source lineage, active fresh status, exact mapped
rendered bytes/hash and default renderer metadata. This consumes 50 additional
read requests per backend: the minimum path is 454 of the unchanged 4096 HTTP
cap, leaving at most 3642 polls and other calls. The existing 262144-byte response,
256 MiB capture, ten-second request and 3600-second preparation bounds remain.
The route's larger product ceiling does not enlarge this collector's limit.
The extra reads are neither measured queries nor paid provider dispatches.

D's separate `workload-rendered-map.json` at `ca057879` has raw SHA256
`9dbcade15daa3fb504838c2fe4f743b4b5fc37da20b2c0f29e2e3aea49ad57ab`.
The manifest and independent review must pin it as `rendered_map_path` and
`rendered_map_sha256`. Original corpus, typed-contract and allocation hashes do
not change. Each source-specific row fixes original input hash/UTF-8 length,
rendered text/hash/length and exact document-root/paragraph/chunk spans. For these
frozen short single-line inputs, the default renderer adds exactly one LF;
the root includes it, while paragraph and chunk spans end before it. Candidate
bytes are compared exactly, never trimmed. The map pins renderer/tree source
hashes and configuration for independent exact-artifact prerequisite review;
a well-formed map by itself proves neither installed source nor actual settings.
The preparation observation reports both hashes and the map identity. Measurement
requires the independently qualified prepared fixture to bind that same map.

Public node markers and `pipeline-status.chunks.embedded` are **not a raw physical
index inventory**. These endpoints also do not expose the original private
SourcePublicationManifest. The full immutable DocRevision is checked through its
new authorized inspection route and its response is retained. Private manifests
and raw vectors remain unmet, and the report always emits
`qualified: false`. D may use the retained raw observations alongside independently
qualified original inventory/gateway evidence; this helper cannot manufacture
the qualified prepared-fixture receipt consumed by the measurement collector.
No direct lookup is passed off as retrieval precision or provider relevance.

G exclusively owns the separate SQLite allocation/control authority and
`gateway/cohort_bridge.py`. Its root-named begin/close functions are loaded by
exact source hash, independently pinned in the manifest and D review. Begin must
admit the explicit `provider_phase: preparation` after local preflight; close runs even on
failure, cancellation or partial begin. No fresh ledger, budget reset or inferred
request-to-provider association is introduced here. The original 432-request
semantic allocation is unchanged. D's separate inactive 2100-physical-attempt/
$3 proposal accounts for SDK and ordinary queue retry capacity; it is a hard
stop ceiling, not guaranteed completion under recovery or extra work.

The supplied independently qualified runtime evidence must bind actual installed
source/archive/binary/backend/endpoint and the effective three-total-HTTP-attempt,
three-ordinary-queue-attempt, no-summary recipe. This is a pinned external input,
not a claim that an arbitrary file proves the installed configuration. Recovery
can exceed ordinary queue attempt counts and still consumes G's original cap.
G independently inspects actual predispatch arrays (1-64 inputs, conservative
8192 total input bound). The helper checks public produced-node observations
after publication; it does not invent unpublished node counts from acceptance.

Each request first persists a private `NNNN.request.json` intent with the original
body bytes/hash, method, path, space, credential role and one-attempt identity.
The file and capture/output directory entries are fsynced before connection.
The intent makes no claim that bytes were sent. Recording failure, expired
allocation or insufficient room for the bounded response refuses before dispatch.
The separate response/error capture names the exact intent hash; neither record
stores the bearer. An interrupted or uncertain POST remains an incomplete original
attempt and must not be submitted again. These records share the existing 256 MiB
capture ceiling; they do not expand any request, provider or time allocation.

Bounds: 3600 seconds preparation/backend, 4096 HTTP requests, 10 seconds absolute
per public request, 262144 response bytes, 256 MiB private captures, and 130 seconds
for terminal accounting. G's original allocation expiry still governs every
dispatch. The helper reuses the tested strict asynchronous HTTP framing parser
and D JSON parser. Credentials are bounded explicit private JWT files; captures
omit bearer values. `inventory` is inert. Offline tests use mocked transports and
synthetic schema controls; they are not candidate, provider or product acceptance.

Passed cohort accounting must reserve exactly 1,064,960 nano-USD for every
physical dispatch reservation, without refunds, as frozen by D/G. Integer fields
cannot be booleans. Offline stalled-close controls exercise an actual absolute
POSIX deadline around a mocked blocking bridge, preserving unmet accounting
and the original independent verdict without opening a socket.
