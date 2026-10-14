# Scoped ranking contract and index controls

The shared HTTP/MCP `SearchRequest` accepts one optional selector:

| Request selector | Candidate discovery and ranking |
| --- | --- |
| neither selector | Hybrid, retaining the existing default |
| `use_vector: false` | Lexical, without resolving a provider |
| `use_vector: true` | Hybrid |
| `ranking_mode: "lexical"` | Lexical |
| `ranking_mode: "semantic"` | Indexed node cosine similarity only |
| `ranking_mode: "hybrid"` | Retained lexical/vector reciprocal-rank fusion |

Both selectors together, explicit null, unknown modes and incorrect types are
invalid. Original query text is unchanged. For example, the public request
`{"query":"How do I shut off the outdoor faucet?","limit":3,"ranking_mode":"semantic"}`
uses the selected named space's current approved query embedding binding.
Revocation or missing approval fails the request; it does not select lexical
retrieval. The semantic score is cosine similarity. Hybrid/lexical scores remain
reciprocal-rank fusion scores and are not comparable with semantic scores.

Source constraints use the same identity bounds as source intake: external IDs
are at most 1024 UTF-8 bytes and revisions at most 256. Neither may be blank or
contain control characters. Labels keep the 128-byte key and 1024-byte value
bounds. Validation never rewrites an identifier or an original query.
Query label lists admit at most 64 values and filters at most 64 keys.

Semantic mode never runs lexical candidate searches or substitutes another node
from a matching document. It merges actual similarity across partitions for the
same approved profile, then resolves the exact indexed node through the existing
current-publication, immutable-span, source-envelope and ABAC checks. Labels and
revision constraints still apply. Unauthorized or stale candidates are discarded
without contributing returned scores. Current approval/profile and source heads
are rechecked before returning. This does not establish provider relevance or
canonical extraction correctness.

Coverage-only and ranked responses also recheck the source authorization
snapshot, including its policy envelope, source version and deletion marker. A
reclassification can therefore invalidate a response even when its source head
has not changed. Storage errors during the final binding lookup remain retryable
`unavailable` errors; a missing approval remains `provider_unapproved`.

Semantic discovery starts with up to 400 raw slots per partition, sharing at
most 4096 slots across at most 64 accessible partitions. A full raw window is
not exhaustion, even if all resolved hits fail authorization, labels or source
constraints. Full windows widen selectively until every partition is exhausted
or the last requested eligible, deduplicated result strictly outranks every full
window's remaining score ceiling. Equal-score boundaries widen because an unseen
tie may sort earlier. A full window without a finite ceiling cannot prove the
rank settled under the storage backend's returned-window contract. HNSW/ANN
window floors are not a mathematical proof of corpus-global nearest neighbors;
actual semantic relevance still requires D's independent provider oracles.
The engine also bounds the union of resolved nodes across repeated
ANN windows to 4096. If retained or reclassified physical rows expose one node in
multiple partitions, its best finite score wins independently of partition
iteration order; higher-scoring copies still undergo current evidence checks.
Failure to establish the rank within these conservative raw
reservations returns typed `rate_limited`; it never returns a falsely complete
empty rank or substitutes lexical results. Already exhausted partitions retain
their reservations, so this cap can reject a request conservatively.

Every returned citation preserves its complete validated immutable support span
and exact half-open UTF-8 offsets. Claim text is complete too; no 1024-byte prefix
stands in for a claim's later predicate/date/negation. The encoded response cap
remains 262144 bytes; a selected response that cannot fit is `payload_too_large`,
not an unrelated excerpt. Lower-ranked oversized evidence does not invalidate a
smaller selected answer. Candidate retention keeps only the requested top unique
results while scanning; eligible candidates and current coverage still undergo
their normal checks. A bounded counting writer measures actual encoded JSON
bytes, including escaping, without allocating a second oversized serialized body.
Coverage-only responses use the same cap. This establishes honest citation bytes, not entailment or
provider relevance.

`index_fixture.rs` accepts revisions, stages their actual documents, renders
immutable text, stores declared input spans, stages metadata through D's fixture,
embeds through the production governed client and `EmbedWorker`, and invokes the
real source finalizer. It does not insert published source heads, final receipts
or canonical answers. Controlled HTTP transports are explicitly spies. The
fixture's fixed in-process principal exercises the actual MCP dispatcher, request
read fence and production ABAC; it is not a
verified HTTP authentication test.
Original finalizer, metadata producer/registration and embedding work carry the
configuration token and its production-derived plan ID. Metadata artifacts carry
the same context, and the fixture verifies the actual completed producer and
registration records before proceeding. Embedding work uses the production
configuration-versioned ID helper. No current configuration is substituted into
an older claim after the fact.

`scoped_index.rs` authors both-backend tests including:

- Known controlled similarities in multiple policy partitions distinguish the
  exact matched node from a lexically matching sibling, global similarity from
  partition-local rank, current revisions from superseded vectors, and visible
  sources from a hidden-policy source. Revocation fences semantic/hybrid calls;
  lexical requests require no provider even after revocation.
- All 28 frozen corpus revisions and 24 original queries traverse the actual
  index. Seeds 17, 219 and 20261006 generate nonzero unit vectors of 384/1536
  dimensions for the two spaces. The SHA256 counter generator consumes only seed,
  source/query ordinal and coordinate, never text. Every seed/backend must fail
  frozen required-passage recall and positive-case macro MRR 0.90 while actual
  published-revision coverage remains complete. Expectations are unchanged after
  results. D must review the generator before execution and judge retained output.

Set `SEMANTIC_CONTROL_CAPTURE_DIR` only to a separately allocated qualification
directory when integration schedules these tests. Captures contain exact shared
engine responses, the frozen corpus digest, generator identity and seed. They
are labelled `indexed-fake-vector-negative-control`, `authentication_enabled:
false` and `product_acceptance: false`. They can support independent retrieval
metric review; they are always ineligible for real-provider acceptance.

This checkpoint contains test source, not an executed Rust or installed-artifact
result. Python's existing nine offline MT controls remain a distinct oracle
self-test. No semantic thresholds or corpus evidence changed in this repair.

Coverage compares opaque revision identities and the current acceptance operation. If A was published, B accepted, then a fresh A accepted under a new operation/generation, the earlier A evidence remains eligible while the fresh operation runs. Coverage for desired/requested A is pending/running/failed until that current operation completes, and the response is partial. An explicitly requested older published revision distinct from the current desired revision can still have complete coverage. Consumers use operation GET for the precise accepted operation’s lifecycle; search coverage does not replace it.

The authored `pending_aba_acceptance_keeps_old_index_evidence_with_partial_coverage` regression publishes actual rendered/embedded/finalized A on both backends, accepts B and fresh A through the production repository with new intent keys and current CAS, then queries lexical and semantic paths with and without the A constraint. It requires old A evidence, partial/pending coverage and an unchanged pending fresh operation. This is test source, not an executed acceptance result.

`semantic_window_reaches_requested_source_after_401_ineligible_nodes` declares 401 higher-similarity chunks in one unrequested source and a lower-similarity requested target, all in one allowed physical partition. Actual render/embed/finalize precedes the query. The 400-to-800 widening fits the budget, so this positive case requires the target and one query embedding dispatch; a budget error does not satisfy it. D separately owns the hidden-label same-partition window oracle and long-claim evidence oracle. None has executed in this lane.
