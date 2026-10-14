# Independent semantic and provider qualification

This lane owns proposed expectations and executable test source. **No authored
fixture, test, dispatch spy, JSON validation or static review is acceptance.** D's
qualification lead approves corpus and thresholds before any candidate results
are judged. The starting joined source is
`6a6f8480d40acd79230b00a2c749ae79c71b20e1`, descended from the handoff base
`f59e6f5c99ed095a015656626f4d32c106c13075`. Existing `tests/recall_preview` is
untouched and retains D lead ownership.

D sealed the expectations before any executable outcome in commit
`b6a40ecc844aec1aa5b2c0b2e0e34063372f956e`, in
[`semantic-expectations-approval.json`](../recall_preview/semantic-expectations-approval.json).
Its raw file SHA256 is
`938d2c0920fb8059500359a6eaa386c7fa83ba59f44040f030bda4fb847a2f9c`;
the approved corpus canonical SHA256 is
`fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b`.
This separate approval record is the expectation authority. The corpus stays
immutable; approval grants no execution or semantic acceptance.

`corpus.json` defines 28 small source revisions, 24 original queries, two named
spaces, and eight exact-quote/unsupported-claim counterexamples. The shared
external key has different content in each space. Household, business,
corroboration, effective dates, cancelled appointments, namesakes, false quoted
training text, negation, part/whole reasoning, and unsupported questions have
explicit expectations. The old chimney revision is ingested first and replaced;
the current run must not mix it into current evidence.

The corpus evidence registry freezes exact UTF-8 supporting subspans per query,
named `external-source-id@revision#query-id`, plus full-note `#0` passages for
unsupported-claim counterexamples. Returned spans may cover a required subspan;
they need not reproduce unrelated sentences. Every passage is below the active
public quote-size bound. The query's required
IDs must occur in the returned immutable evidence; knowing the source ID alone
is insufficient. These are synthetic identifiers, not pre-created canonical
winners. Actual internal IDs are recorded by the later ingest/capture owner.

The proposed fixed metric ranks source/revision pairs by first appearance in the
unchanged result/evidence arrays. Multiple citations in a claim occupy consecutive
source ranks. Duplicate spans cannot inflate ranks. Evidence-ranking P@3 is distinct from the
HTTP result limit: a multi-source result may consume multiple evidence ranks.
A later supporting citation cannot retroactively credit the first rank occupied
by an irrelevant fragment from that source. Precision@3 always divides by
3, including short results; per-case minimum is `len(required)/3`, with all
required passages in those three ranks. Positive-case macro reciprocal rank must
be at least 0.90. Unsupported questions may return unrelated but correctly supported search hits;
these are not invented answers. D review must reject every unsupported answering
claim, including the frozen attic-turbine/patent counterexamples. Their retrieval
score is reported separately from the positive-query denominator. No sparse-query rewriting, direct expected-doc
fetch or advisory score override exists.

Groundedness is separate: every claim/entity result needs exact evidence plus a
separate D review of its actual text, bound by hashes to that result and response.
The review maps supported propositions to predeclared fact IDs and identifies
contradicted/unsupported text. It must review **all** canonical results, including
unanticipated claims, not only recognize desired phrases. No product-supplied
`supported=true`, high confidence or exact quote can grant this judgment. Required
facts must be present and forbidden facts absent. No unsupported claims are
allowed. A response containing only chunks may pass retrieval while leaving
canonical-answer groundedness unqualified; this is intentional separation.
Raw chunk text must equal its verified immutable quote, matching the active
route's chunk contract. A fabricated answer cannot bypass review by changing its
kind to `chunk` while attaching an unrelated valid quote.

Completion checks the real response's per-external-source requested, desired and
published revisions, `state=complete` and `partial=false`. All returned source revisions, including distractors, must also have
truthful complete coverage. It is query coverage, not a claim that the entire corpus completed or a queue Done event is sufficient.
The overall report keeps retrieval, groundedness, completion, evidence eligibility
and individual failure explanations separate. Manifest identity and reviewer
attestations are not cryptographic authentication: D must retain raw receipts and
custody of the exact installed artifact.

## Owned source

| File | What it actually exercises | Limit |
| --- | --- | --- |
| `provider_spy.rs`, `provider_adapters.rs`, `provider_extraction.rs`, `provider_cache.rs` and `../scenarios/tests/recall_preview_providers.rs` | Real configured repositories on SurrealDB kv-mem and ElephantDB; production governed embedding/model HTTP transports and agent-build guards; two interleaved spaces | Loopback spies, no model relevance, no durable disk or installed artifact proof |
| `oracle.py` | Exact captured public response rank/evidence/coverage plus independent semantic review | Offline evaluator; does not produce or authenticate product observations |
| `controls.py` | Seeded finite, nonzero, dimensions-valid random vectors scored against original query expectations | Offline numerical failure control; installed vector-index control is still required |
| `index_fixture.rs`, `scoped_index.rs`, `SCOPED_RANKING.md` | Actual accepted/rendered sources, production embedding worker/index/finalizer and public MCP scoped handler; exact-node/global-similarity checks and seeded random-vector rejection on both backends | Authored Rust tests, controlled spies, fixed in-process ABAC; no executed Rust or installed-artifact acceptance |
| `test_oracle.py` | Mutation tests of evaluator fail-closed rules | Mathematical fixture captures, not product results |
| `../../scripts/qualification/semantic_evaluate.py` | Offline inventory/evaluation/negative-control CLI | Does not start Recall, call a network, or spend tokens |
| `DISPATCH_REVIEW.md` | Current call graph and explicit missing caller/cache coverage | Static evidence only |
| `REAL_PROVIDER_RUN.md` | Frozen inputs, budgets, configuration prerequisites and execution specification | Proposed future authorization, not permission to probe |

Provider scenarios use current `SpaceConfigRepository` on every call, through
`ProviderDispatchContext::with_credentials` from the provider successor source.
No environment mutation or real credentials are used. The internally constructed
extractor uses the provider owner task-scoped credential injection. Warm entity
API tests use exact-profile stored vectors and the same handle after deletion
and embedded restore. The retained public MCP entity-search handler is exercised
with two populated same-dimension spaces, original query bytes, a warmed handler,
and current revocation; they do not establish full source erasure. Most generation spies return a controlled HTTP 400 after recording a call.
The actual extractor test instead uses scripted SSE extraction and a separate
fixed negative support verdict for an exact, independently authored false
acquisition claim. It never publishes a canonical winner. The actual identity
sweep also receives a scripted confidence-0.5 response below its 0.95 merge
threshold: it may persist a pending review candidate, while both identities stay
active; missing approval and a revoked pending retry must make zero unauthorized
calls. These responses are explicitly scripted, and no TM winner or wiki
publication is manufactured as acceptance evidence. Embedding
spies return fixed shape-valid vectors and are always labelled deterministic.
On 429 the spy commits revocation or rotation before returning the response, so
the client's own HTTP retry path must observe it. Missing/wrong-space bindings,
revoked cached handles, agent continuation and changed model requests must yield
zero unauthorized requests. Profile/account/endpoint/model/version/dimension/
preprocessing replacement is different from credential rotation.

There is no automatic run authorization in this directory. At a source-complete
checkpoint, request D/parent validation allocation. Offline evaluator self-tests
can be allocated separately. Later Rust validation must use this worktree's
`./scripts/repo-cargo`, idle scheduling, coordinator job cap and free-disk floor.
No shared targets, cache deletion, paid calls or deployment are needed for spies.

Future commands, **not execution receipts**:

```sh
python3 -m unittest discover -s tests/recall_preview_semantics -p 'test_*.py'
python3 scripts/qualification/semantic_evaluate.py fake-vectors
python3 scripts/qualification/semantic_evaluate.py evaluate capture.json --review review.json
CARGO_BUILD_JOBS=16 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
BINARYEN_CORES=8 TLC_WORKERS=8 chrt --idle 0 ionice -c3 \
./scripts/repo-cargo test -p elephant-scenarios --test recall_preview_providers
```

D retains corpus/verdict ownership. Corrections require a new reviewed version
with rationale before judging new results; candidate output never lowers a gate.

The retained full Alice scenario in `tests/scenarios/src/embedded_elephant_db.rs`
uses the shared controlled HTTP spy with current persisted query/document
approvals. Its original hashed bag-of-words transformation, full Alice text,
cake distractor, query, and ranking assertions remain deterministic regression
inputs. Actual document embedding and query transports must execute, and lexical
fallback cannot satisfy the scenario. This fixture does not establish a real
provider's relevance or source-publication acceptance. The independent frozen
semantic and fake-vector rejection gates remain separate.

Retained MCP ranking fixtures explicitly approve both query embedding and the
document profile used for guarded fixture-vector publication. Removing a test
binding retains its immutable profile history. The existing type-coercion
scenario keeps its default search route and string limit, now with one approved
controlled original-query embedding; fixed vectors carry no semantic-quality
claim. These setup repairs preserve the original ranking and coercion assertions.

The retained S5.7 `search_all` bucket scenario now observes accepted-but-pending
document absence, then drives the actual source dispatcher and finalizer before
requiring its original positive entity/document buckets and exact document text.
Its explicit empty-claim extractor and controlled embedding transport test the
publication/bucket contract only. The original operation must be Completed with
its accepted source generation and revision; quiet work queues alone cannot pass.

Controlled transports use the pinned SDK's provider-specific endpoint convention:
Anthropic receives the origin because the SDK appends `/v1/messages`; OpenAI
receives `/v1`. The spy observes every HTTP method/path, including malformed JSON
or missing credentials, and invalidates unexpected requests instead of treating
router-generated errors as zero outbound calls. Large extraction and identity
test futures are boxed; stack-size changes cannot substitute for their assertions.

The actual indexed fixtures publish Chunk nodes so both retained lexical and
semantic paths can read the same frozen spans. The ABA integration oracle keeps
old published A evidence available in both modes, with and without exact source
constraints, while a fresh A acceptance remains pending. A constrained query
skips retrieval only when no requested immutable revision is currently published;
pending operation state alone cannot hide already-published evidence.

The refusal oracle recognizes both a stream error and the SDK 0.8.51 terminal
`Done(Error)` event. A successful terminal event, generated output before failure
or silent EOF still fails the oracle; exact positive dispatch counts and all
unauthorized zero-call requirements remain unchanged. Identity adapter assertions
still require the provider failure to reach the caller. The active identity sweep
boxes its inner preparation/evaluation/factory/run futures, including fallback,
so boxing an outer test does not leave the nested state machines on its stack.
Actual central runtime validation is still required for this source repair.

The retained API MCP toy flow uses the same current query/document approvals and
controlled transport, scoped around the actual RMCP handler because session tasks
do not inherit a caller's task-local credential resolver. The original transport
origin checks, inputs and positive bucket assertions remain. A pending document
must be absent, and the actual dispatcher must complete the original source
operation before positive document search. Its empty-claim extractor and declared
vectors qualify only this publication/bucket fixture, not model relevance or
signed-token authorization. The shared finalizer must validate its real bundle;
the fixture never supplies a synthetic validation stamp.

The Alice numerical fixture now drives both original source operations through
actual work dispatch and publication before searching. It first requires an empty
pending-document result, then checks both original receipt generations and
revisions, more than ten produced chunks, actual document/query transport calls
and the unchanged Alice-over-cake ranking. The explicitly selected spy mode hashes
each actual embedding request with the original `hashed-bow-v1` algorithm; it
cannot invent a winning document from a query lookup. This remains deterministic
ranking/publication regression coverage, with no real-model relevance claim.

`provider_direct.rs` adds source-only integration regressions for the retained
`AgenticTriage` and `DefaultExtractor` adapters. Both backends and both configured
spaces must refuse missing/wrong authority and revoked repeats without an outbound
call. Each approved attempt must make exactly one correct-model, original-input
call to its own controlled transport. The triage test separately checks its
documented rule fallback after the approved provider refusal; that fallback is
never counted as a generated model decision. The default extractor must propagate
failure without claims. The dependency build exercises the real adapters rather
than their in-crate `cfg(test)` substitutes. D reviews these added expectations
before execution; they are not part of the frozen Runtime6 24-test receipt.

A separate direct-extractor rotation regression requires two approved controlled
generation dispatches per space/backend: the second must use the new credential
while preserving provider, account, endpoint, model profiles and operation
bindings. A subsequent revocation permits no additional call. This adds generation
rotation coverage independently of the existing embedding-rotation test; both
controlled provider responses fail deliberately and establish no extracted claim.

`provider_media.rs` exercises the actual `MultimodalRenderer` live analyzer with a
persisted, exact one-pixel managed PNG on each backend and in each named space.
Document/context mismatch must fail as provider authorization even when the
selected-space asset exists, and must precede lookup of a deliberately absent
asset. Missing/revoked approval permits no outbound calls; the approved original
bytes/title/sidecar must reach the correct model and credential exactly once.
The controlled HTTP refusal must not become rendered text. No test analyzer is
substituted. This source depends on G's document/context guard and requires D's
independent review and central execution; no media relevance is established.

## Claimed reindex adapter source

`provider_reindex.rs` calls the same
`process_embedding_reindex_with_attempt` entry used by the production worker.
The original `WorkItem` is persisted and claimed through the real repository;
the test obtains its fence only from that returned claim. On each backend, two
spaces retain their original active profiles while separate immutable target
profiles use distinct accounts, credentials and dimensions. The test covers
missing staging/current approval, foreign original claim or target, unknown
space, the real empty-node phase, one exact own-space entity embedding request,
durable target input hashes, and revoked continuation without progress or more
outbound calls. Staged vectors must remain unavailable as the active query
profile. Entities are intrinsic transport inputs, not fabricated source claims.

These are new, unexecuted transport expectations for independent D review.
Controlled nonzero vectors do not establish relevance. This direct adapter test
does not qualify HTTP administration, queue scheduling, activation completeness
or real-provider quality, and is separate from the frozen Runtime6 evidence.

## Extraction fixture stack investigation

The central Runtime11 extraction/support dispatch test still aborted with a
stack overflow after the product's inner agent-phase boxing. The test now boxes
its fixture setup/configuration and run-call futures and emits bounded phase
labels with only the synthetic backend/space identity. These labels distinguish
setup, the approved model run, support inspection, revoked support and missing
extraction in the next centrally scheduled execution. Both backends, spaces,
real governed transports, unsupported-claim counterexample and zero-call
refusals remain unchanged. No stack size or timeout was increased. This is an
unexecuted source candidate and diagnostic aid, not evidence that the abort is
fixed or that provider semantics passed.

## Native reindex transaction writer

`native_reindex.rs` exercises the actual native staged-node vector repository
without invoking a provider. Its literal vectors are storage inputs only. The
case requires a successful target write to rotate the existing own-space
canonical epoch while preserving the complete configuration, node and foreign
space epoch. A stale approval or missing original epoch must refuse atomically,
retaining the prior vector. The target remains staged under the original active
profile. This source test does not claim concurrent-overlap qualification; D's
independent deterministic overlap control remains separate.

The source audit distinguishes dynamic `emb__` partition writes, which have no
canonical predicate event, from `entity_profile_embedding` writes, whose real
content effects already share the configuration writer through migration 031
and the private reference guard through migration 039. An unchanged
`processing = processing` assignment alone proves no native write conflict.

## Ordered dispatch input guards

The pipeline composition controls in `provider_dispatch_composition_tests.rs`
exercise the actual observer adapter: revocation during a suspended durable
admission, ordered multiple guards, original refusal, original usage/completion
identity, and guard-only contexts. They are closed-world mechanism controls with
no provider or transport and do not establish provider relevance. Source is
authored for central validation; no local Rust execution is claimed.

## Media original-input custody

The active `MultimodalRenderer` fixtures retain both backends and distinct
household/business spaces. New controls change actual managed payload, full
policy envelope, subject state or lifecycle inside the inherited physical-send
observer and require zero outbound calls. Separate absence controls create an
own-space or foreign physical Doc during that wait. An unchanged input must
still reach its original approved transport. Malformed publication, source link,
wrong expected payload hash/length and unadmitted Work refuse the unmanaged
fallback. Source and Work custody use the owners' existing proof helpers.

The retry control commits a real asset replacement before returning HTTP 429;
the original accounting observer must see two physical-send attempts while only
the first reaches the spy. The completion control supplies an explicitly labelled
transport marker, retains actual original usage/completion callbacks, changes
the asset after the provider's successful completion and requires no rendered
projection. Its unchanged control returns the marker. Neither is a visual or
provider relevance acceptance case. These controls are authored, not locally run;
independent expectation review and central execution remain required.

Media original Work selection also decodes a present `_publication` with the
shared source-queue parser and requires the exact captured source, generation,
revision and operation. A null, incomplete or mismatched descriptor cannot use
another input. The new pure selector control covers all four fields; absent
execution ancestry remains the original storage proof owner's decision and is
never promoted to Independent by this selector. This follow-up is source-only.

## Original and rendered retrieval coordinates

`render_coordinates.rs` reads D's seven literal cases from
`tests/recall_preview/render-coordinate-fixtures.json` at its independently
reviewed raw hash. The fixture supplies both original and expected rendered
bytes before invoking actual source acceptance, default rendering, embedding,
manifest staging and finalization. Actual public MCP and HTTP search must return both exact coordinate systems
and hashes for the selected interval, on both backends and both named policy
spaces. Repeated equal phrases cannot select a
different occurrence. Self-consistent tampering with rendered text/hash cannot
reuse the completed original proof. Six cases use lexical retrieval and make no query provider calls. The second
repeated phrase uses actual semantic node retrieval with explicitly declared spy
vectors, since lexical retrieval selects only one passage per document. It must
make exactly one governed query call for MCP, one for HTTP, and one for the
post-tamper refusal. These vectors exercise coordinate selection only; neither
the controlled ranking nor the resulting passage is provider relevance evidence.

Only the new default-render fixture skips separator-only candidate chunks; it
never clips the selected literal interval or derives expected rendering from
product output. Existing passthrough publication fixtures retain their behavior.
The mapper's independent boundary/identity negatives and HTTP immutable-revision
route controls belong to the coordinated G/D/C source family. These tests are
source-only until centrally executed; literal fixture validity, a spy completion
and source review do not qualify retrieval relevance or workload latency.

D's static review identified the repeated-phrase fixture's earlier lexical
selection error before execution. The corrected case retains the original
second-occurrence expected offsets, quote and hashes and reaches that exact
node through the active vector index. All expected literals remain unchanged;
only the declared controlled transport path and exact physical counts differ.

The final parent-Doc route contract adds required `SearchEvidence.doc_id` from
`resolved.doc.base.id` under the same original current-read and publication proof.
`RecordId` stores a UUID, so this canonical Doc identifier is 40 ASCII bytes;
it is not derived from an external source key or revision text. The existing
constructor serves every evidence origin. The coordinate fixture additionally
checks the returned parent against the actual immutable revision's Doc, while
C's mounted HTTP fixture constructs the immutable GET route from the returned
IDs. D's seven original coordinate literals remain unchanged. This isolated
constructor delta requires C's `4e21275` DTO/wire/test successor; no execution or
consumer delivery is implied by source availability.

Runtime16 still observed an API toy-flow stack overflow after the earlier outer
fixture and pipeline future boundaries. The toy's actual `ApprovedToyServer`
credential wrapper now boxes the inner production MCP dispatcher future before
passing it through the task-local credential scope. Toy client calls also have
an explicit boxed boundary. Stable phase and tool-name diagnostics identify the
next failing boundary without printing source content or credentials. The real
transport, approval, source intake, pending-read and completed-publication
assertions remain unchanged. This is a source candidate pending central runtime
validation; it does not establish that the original SIGABRT is repaired.

Runtime16 S5.7 reached a nonempty published document bucket but its assertion
read `doc.data`, the separate `search_docs` result shape. `search_all` returns
`RedactionEnvelope<Doc>` directly in that bucket. The scenario now deserializes
the actual `UnifiedSearchOutput` type and checks `row.data`, preserving the exact
original title and rendered text including the terminal LF, pending invisibility,
actual embedding dispatch and original completed-operation requirements. This
source correction awaits central execution; it changes no product wire shape.
