# Finite primary provider proposal

This is an unexecuted proposal for final user approval after a runnable artifact
and effective limits are available. It authorizes no spending, credential access,
model downloads, inference or service startup. The machine-readable proposal is
[`primary-provider-proposal.json`](primary-provider-proposal.json). Its independent
expectations remain sealed by
[`semantic-expectations-approval.json`](semantic-expectations-approval.json).
The 28 revisions, 24 original queries, eight unsupported-claim counterexamples,
required evidence, thresholds and independent reviewer do not change.

Workload correction, 23:28 parent clarification: the unchanged v1 JSON's
`approximately-140-measured-overlapping-calls` label is superseded. OB3 reported
about 140 workers over 51 minutes, which establishes neither a request count nor
simultaneous overlap. D28 now freezes 50 total sources across two spaces and
50 callers making four sequential requests each, 200 requests per backend;
actual request overlap is measured separately. Larger or longer consumer
workloads and the original approximately 140-search stress remain unexecuted and
require their own concrete schedules, measured overlap and allocations. This clarification does not change the
provider packet or its seal.

The proposed primary configuration uses actual supported OpenAI transports:

| Space | Embedding profile | Generation |
| --- | --- | --- |
| `semantics-household` | `text-embedding-3-small`, 1536 dimensions | `gpt-6.1-sol`, medium |
| `semantics-business` | `text-embedding-3-large`, 3072 dimensions | `gpt-6.1-sol`, medium |

The existing corpus supplies distinct policies, levels and guidance. Each space
also has distinct profile and connection IDs. The business embedding matches the
HomeCore model/dimension requirement. A single explicitly approved OpenAI account
may serve both spaces, with its real nonsecret identity filled before approval;
this demonstrates space/profile isolation, not independent-account isolation.
Only the credential reference `OPENAI_API_KEY` is proposed. Its reported presence
does not prove entitlement to these models. No secret values were inspected.

The earlier S run specification's Anthropic plus local FastEmbed combination was
a proposal, not an additional mandatory primary-run requirement. Local pinned
FastEmbed, cross-provider/account isolation, Astra consumer-fleet parity, Darwin
native installation and container/PV installation keep separate named gates.
The approved BAAI model choice and its still-missing exact asset provenance remain
unchanged. Their absence must not be called a pass. The primary artifact needs
its actual OpenAI/Meerkat and backend features; it need not load unused local
assets. The separate D28 prequalification and remaining larger consumer workload
obligations are not covered by this semantic-provider allocation.

## Catalog facts and identity limits

The pinned [catalog fact record](provider-catalog-facts.json) records public
official URLs, retrieval time and normalized facts; its byte digest is bound in
the proposal. It is not a full upstream-page archive or remote-weight hash.
Official catalog limits are 1,050,000 context and 128,000 output tokens for both
[Sol](https://developers.openai.com/api/docs/models/gpt-6.1-sol) and
[Astra](https://developers.openai.com/api/docs/models/gpt-6-astra). Standard input /
cache-write / output rates per million tokens are respectively $2 / $2.50 / $10
and $10 / $12.50 / $50. Sol documents medium as its default reasoning effort.

[Small](https://developers.openai.com/api/docs/models/text-embedding-3-small) and
[large](https://developers.openai.com/api/docs/models/text-embedding-3-large)
embedding input costs are $0.02 and $0.13 per million tokens. Their default
dimensions are 1536 and 3072 and maximum input is 8192 tokens per item, according
to the [embedding guide](https://developers.openai.com/api/docs/guides/embeddings).

These are mutable provider aliases. Populate `ModelProfile.model_version` with
an operator-pinned deployment/configuration identity whose receipt explicitly
declares that limitation; do not claim immutable remote weights. Capture actual
request model, returned model identifier where exposed, provider request ID,
timestamp, configuration/approval generation and source/artifact hashes. Remote
model updates require a new qualification capture. Recheck prices before final
approval, without silently updating this frozen proposal.

## Scope and conservative reservation

Each backend gets 28 document-embedding and 48 query-embedding dispatches, the
latter covering 24 unchanged queries in semantic and hybrid modes. Generation
allowances per backend are 56 extraction, 56 support verification, eight summary,
four combined identity/promotion, four truth-maintenance and twelve combined wiki
planner/writer/verifier calls. These are stop limits including every physical
retry, continuation and support pass, not predicted consumption or a requirement
to dispatch unused roles. Budget exhaustion leaves affected cases incomplete.

The active default triage plugin is deterministic. Public manual-source ingest
requests Graph mode; the actual decision and completed plan must be captured.
Do not require a model triage binding when that plugin makes no model call.
Text passthrough rendering likewise needs no media-model approval. Conditional
roles require approvals only when the actual selected work invokes them; omitted
roles cannot be reported as exercised. Freeze bounded identity/TM/wiki operations
before execution and capture their actual outcomes. Missing core extraction or
support work cannot be excused as a conditional operation.

The proposed limits are **432 dispatches, $75, 90 minutes and two simultaneous
dispatches across both backend runs**. Each generation request must have an
enforced upper bound of 32768 total input tokens and 16384 output tokens including
reasoning. Each embedding request is capped at 8192 **total** input tokens across
its entire batch, not 8192 multiplied by arbitrary batch size. Reserving all
generation input at the higher cache-write rate gives $0.24576 per call; reserving
every embedding call at the large model's price gives $0.00106496. The aggregate
worst-case reservation is **$68.97467392**. The earlier $20 proposal cannot cover
these bounds. Actual consumption may be lower, but no cache discount, unknown
usage or failed request is treated as free. Standard nonregional pricing and no
hosted paid tools are required; other tiers/rates need a new reviewed reservation.

The explicit [gateway proposal delta](gateway-proposal-delta.json) preserves this
corpus and budget while reflecting the actual credential topology. Document and
query embeddings share one active profile, so the gateway authenticates their
combined allowance of 76 calls per backend across both spaces. Independent worker
receipts must still show all 28 document and 48 query inputs; the gateway does not
claim separate enforcement of those operation counts. Generation capabilities
require distinct reviewed connections/profiles for their actual role groups.
The original proposal's generic connection IDs are not a runnable gateway mapping.

The active extractor requests 64000 output tokens, which exceeds the proposed
16384 allowance. The delta therefore describes an explicit optional gateway
transform: `cap_at_16384`, requiring a separate run-approval reference. The strict
default remains rejection. The selected mode can lower a valid requested cap;
it cannot repair malformed values, increase a smaller cap or change product
defaults. Preserve original and forwarded bodies and both caps in the receipt.
Any resulting truncation, missing extraction or absent evidence fails the same
frozen completeness and quality expectations. The transform does not establish
unmodified direct-production parity or solve opaque SDK continuation.

The physical configured endpoint becomes `http://127.0.0.1:43129/v1`, with a fixed
upstream of `https://api.openai.com/v1`. Record the physical endpoint in the actual
profile/partition configuration; do not call that direct-endpoint parity.
The separate upstream transport/capture ceiling is 8 MiB per physical dispatch.
The public search response ceiling remains 262144 encoded bytes. Exact synthetic
request bodies, disclosed forwarded bodies and actual response bytes stay in
private files; credential headers are excluded. Full body capture is fsynced
before a completed receipt. Over-limit or short HTTP framing is incomplete, never
a successfully clipped response. At 432 calls, the response plus two bounded
request copies total at most 3737124864 bytes, plus metadata. This is a reviewed
finite capture proposal, not permission to retain arbitrary real user payloads.

D's 17 independent offline gateway contracts passed against source `41cf48fc`
using actual HTTP framing parsing over synthetic byte streams and disabled
networking, including the optional output-cap transform and its separate approval
reference. That result does not prove the proposed input-token framing constants,
the installed isolated runtime, role mapping, SDK continuations or live semantics.
Those readiness fields remain explicitly false in the delta. Check its arithmetic
without executing any gateway or provider:

```sh
python3 scripts/qualification/recall_preview_provider_plan.py --gateway-delta tests/recall_preview/gateway-proposal-delta.json
```

The offline arithmetic checker performs no dispatch and proves no enforcement:

```sh
python3 scripts/qualification/recall_preview_provider_plan.py
python3 -m unittest discover -s tests/recall_preview -p 'test_*.py'
```

## Unresolved installed enforcement and custody

Source review found the governed OpenAI transport uses Responses and retains
physical-request approval checks. Its builder clears `provider_params` and single-shot helpers omit a caller tag,
but further tracing resolves the medium-setting concern: the pinned OpenAI
Responses adapter (`vendor/meerkat-openai-0.8.51/src/client.rs`, request-body
builder) explicitly inserts `reasoning.effort="medium"` for catalog-recognized
reasoning models. This source behavior still needs an installed wire capture. The
`meerkat-models` 0.8.51 source catalog contains both proposed consumer model names;
catalog recognition alone does not prove an installed request succeeds.

`ProviderDispatchObserver::before_dispatch` receives provenance but not the full
request or token bounds. The durable usage observer records calls and later
usage; those records alone cannot enforce this monetary budget. The installed
run needs a trusted pre-send gate that atomically reserves the conservative cost
and a physical-request boundary that verifies token/output limits, model,
endpoint, tier, tools, operation count and deadline. Include background activity,
retries and agent continuations. Persist reservations before sending; after a
timeout or crash, retain unknown usage at the reserved amount. Two concurrent
calls must not overspend the last allowance. Any provider/account limit used as
backup must be verified as a hard limit, not a billing alert.

Before spending approval, independent denial evidence must show zero sends for
exhausted money/count, oversized actual input/batch, excessive output, wrong
model/tier, revoked approval and expired window; prove reservations survive
restart. A watcher that kills the service after usage arrives is insufficient.
No such installed enforcement receipt exists at this checkpoint. These are
explicit run prerequisites sent to the provider/recovery owners, not D product
edits or a claim that a planning checker protects a live run.

Retain exact native archive/container digest, source SHA, features, schema,
sanitized configuration, account identity, corpus and expectation seal, all raw
requests/responses and physical-call receipts. Both backends require isolated
stores. Kv-mem conformance cannot stand in for persistent installed behavior.
Keep the original independent semantic-only and hybrid scoring/review procedure,
and the separately declared seeded actual-index negative controls. Changing the
real embedding dimensions does not retroactively relabel existing 384/1536
offline control receipts; any installed control must record its actual matching
configuration and generator before execution.
