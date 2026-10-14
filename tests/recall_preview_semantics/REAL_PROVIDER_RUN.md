# Future bounded real-provider run specification

Status: **not authorized or executed**. No credentials, model downloads, builds,
services, load, provider calls or deployments are performed by this specification.
D's separate expectation approval is recorded in
[`semantic-expectations-approval.json`](../recall_preview/semantic-expectations-approval.json);
its source and hashes are listed in this directory's README. Parent
separately authorizes a named installed artifact, exact provider configuration,
spend cap and time window. A source commit or offline control cannot close this
gate. No consumer production data is needed.

## Current source assembly and unresolved execution prerequisites

The concrete public collector is B's
`scripts/qualification/primary_provider_driver.py` at source checkpoint
`669eac2358fb9e53b1f0475e2d00fbd834d33a1b`. It uses the original 28 revisions and
48 query requests per backend, typed classified Graph intake, original operation
identities and actual public reads. It does not launch services or mint a
qualified runtime receipt. C's package source at
`2722209bf89cedc5300457fe2db035cd4f668810` assembles reviewed binaries, Python,
libraries, harness dependencies and provenance without executing them.

An actual in-namespace bootstrap/receipt producer still must supply B's reviewed
runtime and G launch-context contract. The candidate must include C's explicit
classified-space bootstrap (`31841b69ab330c99a7a4dbc2605ca96d1790ecfa` or its
integrated successor): singleton named management tokens with the frozen policy
clearance create the spaces and grants; ordinary data tokens still require those
current grants. A global management token supplies no classified data clearance.
The exact artifact, full transitive Python-source hashes, launch/context records,
configuration readback and D's conditional identity/TM/wiki case dispositions
must be pinned before executing the collector.

B's source requires the separately versioned `full-context-terminal-v2` accounting
context described in D's
[`TERMINAL_SETTLEMENT_REVIEW.md`](../recall_preview/TERMINAL_SETTLEMENT_REVIEW.md)
and G's `gateway/TERMINAL_SETTLEMENT_CONTRACT.md`. That candidate preserves the
actual SDK output cap, reserves the full reviewed context before dispatch and
releases unused liability only after strictly validated durable terminal usage.
Incomplete, unknown or late captures retain their full reservation. It introduces
no token-count endpoint calls and does not guarantee completing all 432 physical
attempts within $75. It requires its own exact reviewed manifest and ledger mode;
the original v1 proposal below cannot authorize v2 or vice versa.

C's single authorized host isolation probe failed with
`bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted`. Actual isolated
execution remains blocked until the host prerequisite is resolved separately.
No host-network fallback or weaker isolation qualifies this specification. The
D28 embedding-only 2100-attempt/$3 proposal and the full 140-caller workload are
separate allocations; neither adds calls, time or money to this primary run.

## Inputs and identity to freeze

Use exactly `corpus.json` version `recall-preview-semantics-v1`. Record its canonical
JSON SHA256 using `oracle.digest`; retain the source bytes too. Ingest the 28 listed
source revisions in array order, including `home/visit` r1 then r2 under source-head
CAS. Use two explicit spaces, `semantics-household` and `semantics-business`, with
the distinct policy/level/guidance specified in the corpus. Exact supporting subspans are frozen in the corpus evidence registry. No padding, query anchors or expected-fact strings may be added to input.
Retain original source text, request, accepted operation/head, exact revision,
internal source/doc/node mapping, final receipt and timing. Do not insert graph
winners directly.

Execute on both retained backends with fresh isolated stores. Each run names the
source SHA, installed native archive/container SHA256, all relevant feature flags,
backend and schema version, configuration hash, fixture hash, expectation-review
receipt and sanitized processor bindings. Run labels distinguish SurrealDB kv-mem
source conformance, persistent SurrealDB installed qualification and ElephantDB
snapshot qualification; they are not interchangeable. Native Darwin ARM64 and
container/PV acceptance remain separately scheduled D gates.

## Primary provider proposal awaiting the D run manifest

D owns the exact primary proposal and machine manifest in
[`PRIMARY_PROVIDER_PROPOSAL.md`](../recall_preview/PRIMARY_PROVIDER_PROPOSAL.md)
(source checkpoint `1e5f28381f401b39cb1a5e488bf3e7536017b8c8`, plus its adapter-review
follow-up). D proposed the following configuration before any candidate output. It
supersedes this document's earlier mixed Anthropic/FastEmbed primary proposal;
local FastEmbed and cross-provider qualification remain separate required work.
The frozen corpus, original queries and score thresholds are unchanged.

| Space | Embeddings | Generation | Isolation being measured |
| --- | --- | --- | --- |
| household | OpenAI `text-embedding-3-small`, 1536 dimensions | Approved OpenAI `gpt-6.1-sol`, documented default medium reasoning | Household space, profile, connection identity, policy and guidance |
| business | OpenAI `text-embedding-3-large`, 3072 dimensions | Approved OpenAI `gpt-6.1-sol`, documented default medium reasoning | Distinct business space, profile, connection identity, dimensions, policy and guidance |

If one explicitly approved account is available, both spaces may use that actual
account. Distinct connection IDs do not establish distinct account isolation.
Record the actual account and endpoint identities, model/deployment identifiers,
remote alias mutability, provider catalog capture/hash and approval generation in
D's manifest. No immutable weight claim is justified by a remote model alias.
The governed builder clears caller provider parameters, but the retained OpenAI
0.8.51 adapter itself inserts `reasoning.effort="medium"` for catalog-recognized
reasoning models, including Sol and Astra. This adapter source supersedes the
earlier inference that clearing parameters removed all wire-default guarantees.
The exact installed wire capture must still verify the setting before acceptance.

Credential references proposed for the isolated run are
`SEMANTICS_HOUSEHOLD_OPENAI_API_KEY` and `SEMANTICS_BUSINESS_OPENAI_API_KEY`.
Store references and generations only; never put values in captures or fixtures.
Explicit current approvals are required for query/document embedding, extraction,
support verification, summary, promotion/identity, truth maintenance and every
wiki role. Current default triage is deterministic and spends zero model calls.
MediaRender remains controlled conformance only for this text corpus.

`text-v1` preprocessing, model/version, provider, actual account, endpoint,
dimensions, space and profile identity must all be recorded. Credential rotation
is a separate generation change. Do not permit a process-global credential/model
fallback, including on warm cache hits and retries.

## Local model and cross-provider follow-up prerequisites

D independently approved BAAI/bge-small-en-v1.5 model choice, with Recall profile
`model="bge-small-en-v1.5"` and 384 dimensions. This did not approve unspecified
converted weights or authorize inference. Before a separate local-provider run,
D must freeze the exact upstream/exporter/converter revisions, pooling/output
settings, model/tokenizer file hashes and asset manifest digest. Do not substitute
an arbitrary converted or quantized checkpoint.

That named native/container artifact must include `rocksdb,fastembed` with its
feature/dependency receipt. The connection endpoint is an already provisioned
`file:///absolute/path`; the profile version is `sha256:<manifest-digest>`.
`validate_fastembed_assets` must pass without downloads/inference. The source
validator and model name do not prove installed inference. Model provisioning,
local inference and cross-provider generation each require a separately bounded
approved run; none is authorized here.

## Original v1 hard-budget proposal (inactive)

Per backend, maximum **216** dispatches, counting every retry, continuation,
support pass and background call within the same category allowance:

| Category | Maximum calls per backend |
| --- | ---: |
| Document embedding (28 input revisions) | 28 |
| Query embedding (24 original queries in each of two ranked modes) | 48 |
| Default deterministic triage | 0 |
| Extraction | 56 |
| Independent support verification | 56 |
| Summary | 8 |
| Identity/promotion disambiguation combined | 4 |
| Truth maintenance | 4 |
| Wiki planner/organizer/writer/verifier combined | 12 |
| Total | 216 |

Combined proposal: **432 dispatches**, **USD 75 stop cap**, **90 minutes**, at most
**two concurrent outgoing calls**, standard tier, no paid hosted tools. Per
actual generation request, bound total outgoing input to **32768 tokens** and
output to **16384 tokens**; embeddings to **8192 total input tokens per batch**.
These are proposed enforceable limits, not measured usage or authorization.

D's pre-output pricing proposal reserves USD 0.24576 per generation dispatch
using its captured conservative input/cache-write rate and output rate, plus
USD 0.00106496 per embedding dispatch at the larger embedding rate. For 280 maximum
generation and 152 embedding dispatches across both backends, the proposal totals
USD 68.97467392 before the USD 75 stop cap. The approved manifest must freeze exact
rate semantics and arithmetic from its retained official catalog snapshot before
a run; a document estimate never enforces spend. Unknown usage is unknown, not
zero. Reserve before each dispatch, including retries, and stop before a request
can exceed the remaining reserved budget.

The category counts above describe the planned workload. The current gateway
candidate enforces a combined **76 embedding calls per backend**, shared across
both named spaces and query/document direction; it does not independently enforce
document 28 versus query 48. Recall's active embedding profile and credential are
shared between those operations. D's approved manifest must reflect that exact
physical boundary; logical schedules are not separately enforced wire quotas.

A qualification-only gateway candidate is owned by grounding under
[`gateway/README.md`](gateway/README.md), at source checkpoints `fa69f64` and
`15a4658`. Its reported offline tests are separate from installed enforcement or
semantic acceptance. It remains unready without D's independent token/framing
bound, reviewed configuration topology, installed egress/isolation proof, capture
approval and final explicit spend authorization. Its initial strict refusal of
opaque continuation references can make installed SDK continuation flows
incompatible; this cannot be silently treated as full workload completion.

**Readiness gap:** the existing provenance observer does not receive complete
request bodies or token bounds, so it cannot enforce this money cap. Root/D must
name and qualify an actual trusted outgoing transport/gateway limiter, including
agent continuations, SDK retries and output limits. A loopback gateway is only a
proposal until installed egress isolation proves Recall cannot bypass it. Its
actual endpoint/account/credential provenance must be recorded honestly. A
`previous_response_id` or other server-held context reference can add billable
input absent from the current body; reject those references or conservatively
account their full context, including encrypted reasoning replay and remote
file/image/audio/tool inputs. Scheduler token estimates,
approval lists and provider account estimates do not replace that limiter.
Exhaustion or unsupported limits produces an incomplete result; do not skip cases,
reduce semantic thresholds or replace providers after seeing outputs.

Expected timing proposals: all tiny-note operations complete within 15 minutes
per backend; original-query p95 at most 5 seconds, each query at most 15 seconds;
model hard deadline 120 seconds within renewed leases. Output is bounded to
262144 JSON bytes. Submit original queries sequentially and retain failures in
latency samples. This is separate from the approximately 140-concurrent-search
workload gate. Recurring wiki/reconciliation and unrelated schedules stay outside
this isolated window; required bounded wiki/TM/identity cases stay inside it.

## Ordered execution and capture

1. Verify authenticated readiness without model dispatch, exact artifact/config
   identities, free-space/resource allocation and clean stores. Missing approvals
   are reported before ingest. Do not use SKIP_AUTH.
2. Ingest every listed revision through the real source-first API with explicit
   source-head preconditions. Await real final receipts. Record pending/failed
   operations; do not replace source text, direct-write canonical records or count
   accepted/queued as complete. Execute the bounded identity/TM/wiki cases through
   retained active paths; do not make fixture-supplied winners.
3. Submit every original query verbatim with limit 3, explicit space, and no
   relevance-narrowing source filter. Collect requested revision coverage for the
   **whole current space inventory**, not only known relevant sources. Coverage
   constraints must be confirmed to report readiness rather than filter the rank
   candidates. If they filter, obtain inventory readiness separately and search
   unconstrained. An expected-source-only candidate set invalidates relevance.
4. Capture actual semantic-only and hybrid rankings separately, preserving the
   original result and citation order. Do not infer semantic-only from
   `use_vector=true`, which selects hybrid retrieval. The shared scoped request
   uses `ranking_mode: "semantic"` for actual indexed-node similarity and
   `ranking_mode: "hybrid"` for the separate combined run. Omit `use_vector` when
   supplying `ranking_mode`; sending both is invalid. The authored semantic path
   merges comparable cosine scores across eligible partitions and returns the
   matched immutable node, without sibling-node lexical rescue. Its presence and
   behavior in the exact installed artifact must still be qualified. Store raw
   response bytes/hash, original request, latency and errors. No rescue queries or
   expected-document fetches may affect output or judge context.
5. D independently reviews every returned canonical claim against only its
   returned immutable evidence. Record response/text hashes, fact IDs and explicit
   supported/unsupported/contradicted judgments. Review generated wiki statements
   the same way. Exact quote matching alone does not establish entailment.
6. Score frozen P@3, required passage recall, positive-case macro MRR, groundedness
   and completion independently. Keep semantic-only provider relevance separate
   from hybrid service usefulness. Report every missing/failed case. A complete
   capture is still unqualified until D verifies artifact/provider receipt custody.
7. In a separately isolated test index, repeat the semantic-only route with seeded
   random unit vectors for both documents and queries, preserving dimensions and
   all other inputs. `scoped_index.rs` authors the source-level seam: controlled
   loopback vectors traverse the governed document embedding client, actual
   `EmbedWorker`, physical node index, source finalizer and shared scoped search.
   There is no public fake-provider approval or independent search implementation.
   The source test is not an installed-artifact receipt. Installed control
   allocation must preserve equivalent governed injection and explicitly record
   its test-only configuration separately from the real-provider configuration.
   All seeds `(17,219,20261006)` and dimensions `(8,384,1536)` in the Python offline
   control are frozen. The authored index control uses the same seeds and the
   two configured dimensions, but a separately declared SHA256-counter generator;
   D reviews this generator before execution, without replacing the offline
   control's retained results. The numerical semantic threshold must fail, even before the
   evidence-class rejection. Preserve clean-index pre/post identities. Never
   install fake vectors into the accepted store.

The offline fake-vector control tests the evaluator's numerical expectations.
The Rust actual-index tests remain authored and unexecuted at this source
checkpoint. Neither authored source nor the offline control satisfies step 7's
installed-index control.

## Capture and independent review format

The offline evaluator consumes:

```json
{
  "manifest": {
    "evidence_class": "real-provider",
    "source_sha": "<40 lowercase hex>",
    "artifact_sha256": "<64 lowercase hex>",
    "configuration_sha256": "<64 lowercase hex>",
    "provider_receipts_sha256": "<64 lowercase hex>",
    "corpus_sha256": "<oracle.digest(corpus)>",
    "expectations_review_sha256": "<D pre-execution approval receipt>",
    "backend": "surreal-db",
    "authentication_enabled": true,
    "query_mode": "semantic-only",
    "fallback_fetches": 0,
    "query_rewrites": 0
  },
  "cases": [{
    "id": "h-stop-water",
    "query": "How do I shut off the outdoor faucet?",
    "space": "semantics-household",
    "fallback_fetches": 0,
    "response": {"results": [], "coverage": {}, "partial": false}
  }]
}
```

This intentionally incomplete example cannot pass. Every corpus query is required.
`response` is the exact decoded wire object, using
`results[{kind,text,evidence:[{source_id,revision,span:{start,end},quote}],score}]`
and `coverage` keyed by external source ID, with requested/desired/published
revision and state. Extra supplied quote hashes are verified if present; absent
hashes are never invented as server integrity receipts.

D's separate review object has `status=approved`,
`reviewer="qualification (independent D lane)"` matching the appointed identity
in the expectation seal, exact
`corpus_sha256`, `capture_sha256`, and `expectations_review_sha256`, plus one entry
per query under `cases`. Each entry names `id`, `response_sha256`, and `claims`.
Each reviewed result has `result_index`, `text_sha256`, `verdict`, `facts` and
`evidence_ids`. Review must enumerate every claim/entity and its complete factual
content, including unexpected assertions. Hashes identify evidence; they do not
make the review self-authenticating.
Changing the appointed reviewer requires a separately approved D protocol update;
arbitrary nonempty reviewer names do not grant eligibility.

No full consumer adoption, production cutover, source erasure or restore gate is
claimed by this run. OB3's physical deletion plus bounded backup retention gate
remains independent, and Recall's durable erasure journal/restore barrier remains
an implementation requirement.
