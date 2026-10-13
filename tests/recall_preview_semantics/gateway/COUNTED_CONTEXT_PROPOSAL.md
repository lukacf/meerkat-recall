# Counted stateless context: offline successor proposal

Status: **design for independent review; not implemented or authorized for live
execution**. This document changes no gateway code, manifest, corpus, semantic
threshold or spending authorization. The strict gateway at `41cf48fc7caa5d64106644861bbb03b4e44a1215`
still rejects reasoning replay. Its 29 local and 17 independent offline checks
do not establish encrypted continuation compatibility or a complete token bound.
The original 432-request/$75 proposal remains unchanged.

This proposes a separately versioned gateway manifest and count/forward protocol.
G owns a future implementation; D owns independent acceptance and the revised
qualification packet; B owns installed role/profile bindings. Implementation and
paid execution are separate decisions. No provider request, credential access or
isolation launch was performed to prepare this design.

## Evidence and remaining uncertainty

The official [token-count guide](https://developers.openai.com/api/docs/guides/token-counting)
describes counting complete Responses inputs, including tools, schemas and model
framing. Local plaintext tokenization does not establish the size of encrypted
context. The [count reference](https://developers.openai.com/api/reference/resources/responses/subresources/input_tokens/methods/count)
documents an authenticated `POST /v1/responses/input_tokens`, reasoning inputs
with encrypted content, and a response containing `object` and `input_tokens`.
For streaming replay it calls for the completed reasoning item. It documents no
request digest, deployment identity or certificate binding a subsequent forward.
These pages were reviewed on 2026-10-06 UTC; they do not establish this account's
model/cipher compatibility or a prospective charge bound for counting.

Local SDK evidence is `vendor/meerkat-openai-0.8.51/src/client.rs`: the ordinary
Responses path uses `store=false` and requests encrypted reasoning. Stateless
replay emits reasoning ID, summary, optional encrypted content and optional
phase. Stored continuation is a separate path. Assistant-message phase is
supported by the count schema; **reasoning-item phase is not listed**. That
conditional SDK shape remains unsupported until specifically resolved, without
stripping it. This is source inspection, not installed transport evidence.

The following protocol is an engineering proposal. Local hashes can establish
our routing and byte identity; two provider calls remain non-atomic. An alias or
hidden framing change between count and generation remains possible. A later
usage mismatch detects a failure after spend and cannot prove a hard ceiling
retroactively. D must approve the reliance on provider counting semantics or
require a separately justified worst-case generation reservation. This document
supplies neither an upstream atomicity guarantee nor that alternative bound.

## Frozen request and exact projection

Before any count connection, validate the entire generation request, apply only
the separately approved output-cap/default-field transforms, and freeze the
exact bytes that would go to `/v1/responses`. Retain original and transformed
bodies, hashes and transform names. Counting never authorizes another mutation.
Derive and freeze the count body with a named projection version, using these
candidate rules; missing optional fields stay missing unless an explicit rule
below adds them to **both** bodies.

| Generation field or shape | Candidate count treatment |
| --- | --- |
| `model`, `input`, `instructions`, `reasoning`, `text`, `tools`, `tool_choice`, `parallel_tool_calls` | Copy complete validated values without content substitution. |
| `truncation` | Require `disabled`; if absent, add `disabled` to both bodies with a recorded transform. Reject `auto`. |
| `max_output_tokens`, `stream`, `store`, `include`, `service_tier` | Retain in frozen generation; omit from count under this explicit projection. They retain the existing generation restrictions, including standard tier and `store=false`. D must review the projection against the installed request. |
| `temperature`, `top_p` | Refuse in this candidate until their omission from the count schema is specifically reviewed. Do not silently ignore them. |
| `previous_response_id`, `conversation`, prompt IDs, item references, compaction, remote schemas, files, media, hosted tools | Refuse even if some are supported by the provider. This candidate requires complete inline context. |
| Unknown top-level or nested fields | Refuse before counting; no permissive pass-through or best-effort stripping. |

Retain the existing bounded text-message/function-call/function-output/tool/schema
validators, plus a narrowly reviewed stateless reasoning shape. Preserve assistant
message phase exactly. A reasoning item requires `type=reasoning`, a nonempty ID,
summary entries of `summary_text`, and nonempty encrypted content. Optional inline
`content` contains only `reasoning_text`; optional status must be `completed`.
Reject `phase` on reasoning, unknown fields, incomplete status, ID-only items,
null/empty ciphertext and compaction ciphertext. These are candidate workload
restrictions, not claims that the provider never accepts other inputs.

An ID accompanying a complete inline item must not trigger server-state lookup.
For reasoning, require an earlier complete private capture in this same run and
authenticated backend/space/role/model/account scope that contains the identical
ID and encrypted payload from a completed output item. The gateway, not a caller
header, establishes that association. When the SDK omits `status`, this trusted
terminal record supplies completion evidence. Preserve and count the actual SDK
summary/content; do not replace it with a reassembled alternative. Completed
reasoning from a partial/failed response cannot enable a continuation under this
qualification protocol. Other message IDs remain refused until a concrete
installed request requires and independently justifies full-inline ID handling.

Ciphertext provenance prevents substitution from another qualification context;
it is **not** a plaintext-token bound. Ciphertext length never stands in for
decoded context size. A captured predecessor is not a reusable count result:
every generation attempt, including every SDK retry or continuation, needs a new
count of its entire actual context. No cross-request count cache is proposed.

## Durable pair and one permitted forward

The count route is internal to the trusted gateway. Workers continue to receive
only their scoped proxy capability, not an arbitrary counting endpoint or real
credential. The fixed upstream host/TLS checks, no redirects/retries, private
captures, isolated launcher, authenticated role bindings and approval checks
remain mandatory.

Each pair records: run and pair IDs; original and frozen generation hashes;
count-body hash and projection version; exact backend/space/operation capability
binding; model alias, reasoning configuration, output cap and transforms;
account/project and trusted credential-generation identifier; manifest, approval,
installed configuration/artifact and gateway hashes; original run deadline and
pair deadline. Record a nonsecret credential-generation identifier, never a key
value or reusable credential fingerprint in receipts. A credential change
invalidates the pair rather than transparently forwarding with another key.

1. **Prepared/reserved:** fsync bodies and atomically reserve the count's known
   worst-case charge, the entire generation allowance, one count slot and one
   generation slot, including the original role quota. This proposed conservative
   pair reservation is nonrefundable even if generation never starts. Receipts
   distinguish reserved slots from actual sends; they must not invent a physical
   generation request after a failed count. Missing charge authority refuses here.
2. **Counting:** durably record count-send intent before any connection. Recheck
   current approval, fixed configuration, credential generation and deadlines
   immediately before sending the immutable count bytes. No automatic retry.
3. **Counted:** accept only a complete successful bounded JSON response with exact
   `object=response.input_tokens` and an integer `0 <= input_tokens <= 32768`.
   Reject booleans, floats, duplicates, unknown result fields, missing/malformed
   counts, over-limit values, HTTP errors, truncation and ambiguous completion.
   Persist exact response bytes/hash, result, times and bounded allowlisted
   upstream request identifiers; fsync before making the pair forwardable.
4. **Forwarding:** verify both stored body hashes and all pinned scope/approval/
   configuration/credential/deadline facts again. Atomically consume this pair's
   single-use forward permission and record generation-send intent before any
   connection; check revocation again immediately before sending. Forward the
   already frozen bytes, never rerun request construction. A second consumer loses
   the compare-and-swap and sends nothing.
5. **Terminal:** preserve byte-identical generation JSON/SSE/errors and existing
   incomplete/capture rules. Record actual usage when available, compare it with
   the accepted count/bounds, and halt on disagreement or model inconsistency.
   Do not refund reservations, reinterpret partial output as a successful oracle,
   or pretend post-response checks prevented already incurred spend.

Any crash with a send intent or unresolved pair halts resume for review, consistent
with the current ledger; it never resends or creates a fresh budget. Capture or
journal failure halts. Revocation after count blocks generation. A body/scope/key
change requires a new pair and fresh reservations; expiration never extends the
original run. No client-provided receipt can authorize forwarding.

## Proposed limits and unresolved economics

These numbers are a **new proposal**, not an update to current authorization.
Reserve one pair slot across count and generation, with at most two simultaneous
pairs/upstream sends. Proposed pair deadline is 120 seconds total, including a
count sub-deadline of at most 30 seconds; both are bounded by the original
5,400-second run deadline. No extra time is granted for counts or retries.

| Request category | Existing maximum | Successor candidate maximum |
| --- | ---: | ---: |
| Responses generations | 280 | 280 |
| Responses counts | 0 | 280 |
| Embeddings | 152 | 152 |
| Total physical attempts | 432 | 712 |

Every generation role receives at most one count reservation per generation
reservation; original per-backend generation quotas and combined embedding
quota 76 remain. Failed counts consume their paired allowance. A retry is a new
pair within these limits, so failures can prevent completion of the corpus;
there are no 280 additional free retry counts. Embeddings receive no count-route
benefit and still require independent review of their complete input bound.

The original generation/embedding reserves total **$68.97467392**. The difference
from $75, **$6.02532608**, is not an approved counting allowance. If `C` is an
independently established worst-case nano-USD charge for one count, the same $75
ceiling would require `68,974,673,920 + 280*C <= 75,000,000,000`, as well as a sound
generation reservation. No value for `C` is supplied or assumed. If counting is
billed on decoded tokens, the permitted 131,072-byte ciphertext body does not
establish its pre-count charge. Documented rate/account support and a prospective
count-charge bound are prerequisites before the **first count**, not checks that
can wait until a count result exists. A larger cap requires separate authorization.

Proposed body limits stay 131,072 bytes for incoming/frozen generation and each
count request; an oversized projection refuses. Count responses are at most
65,536 bytes including errors; generation responses retain the separate 8 MiB
ceiling. Added count request/response capture maximum is
`280 * (131072 + 65536) = 55,050,240` bytes, plus bounded metadata. Retain private
original, transformed and count payloads/results under the existing synthetic
artifact custody rules; sanitized operational logs contain no raw bodies or
credentials. Response/body storage bounds are not token or price proofs.

## Required independent offline acceptance

No tests in this list are claimed executed. D should own an independent oracle
once a separately authorized implementation exposes immutable `CountPlan`, durable
`CountReceipt` and single-use pair transitions. Use fake upstreams and deny real
socket construction. Required cases are:

- Actual pinned SDK continuation shapes, preserved assistant phase and encrypted
  bytes, omitted status backed by terminal capture, tool schemas/results and
  non-ASCII text; exact projection and frozen-byte hashes. Reject reasoning phase,
  foreign/partial ciphertext, unknown fields, remote references and truncation.
- Malformed, noninteger, duplicate-key, over-limit, oversized, short-framed,
  delayed/error/ambiguous count responses: zero generation sends and retained
  count reservations/captures. No hidden network retry.
- Mutated generation bytes, count projection, model, account, credential
  generation, role, space, configuration or approval between stages: no forward.
  Simultaneous consumers permit one send; reuse cannot bypass quotas.
- Crash/capture/fsync failures at every durable boundary, missing/corrupt ledger
  and anchor, resumed inflight count/generation, wall-clock rollback, deadline
  expiry and revocation after counting: no reset, refund or ambiguous replay.
- Independent 712-attempt/per-role/two-pair/deadline arithmetic and exact prospective
  charge reservations; unresolved count price blocks the first connection.
- Count/generation usage disagreement and terminal/incomplete streaming: preserve
  the provider result and report failure without manufacturing spend proof.

Later installed isolation/configuration checks and an explicitly authorized
account/model/cipher compatibility exercise are additional gates. Neither fake
upstream tests nor a successful compatibility exercise alone establish the
provider's count-to-forward guarantee. Frozen semantic oracles and their required
quality remain unchanged throughout.
