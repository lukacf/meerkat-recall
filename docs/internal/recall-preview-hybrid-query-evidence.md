# Hybrid document votes and Query wiki source evidence

This source checkpoint descends from product29
`27917ee77852b1cfe84161fd4f216743339a6d5a`. The CLOSED provider29 run failed:
its `h-delivery` mandatory top-three passage was absent in hybrid mode, and all
four conditional cases stopped at caller/lineage predicates. The semantic query
rows met their expectations. The first three failing hybrid scores exceeded
`2/60`, the maximum for one lexical and one semantic reciprocal-rank vote.
The source loop added a vote for each vector node and each partition. This proves
a generic multiplicity defect; it does not prove a future relevance pass.
Original captures, receipts, frozen questions, source text, limits and history
remain unchanged.

Scoped hybrid retrieval now validates exact current evidence before accepting a
semantic contribution. It selects each document's best eligible node, orders
all eligible documents by similarity under the one approved query profile, and
adds one semantic reciprocal-rank vote per document. Equal scores have stable
record/evidence tie breaks. The returned semantic passage is that selected
node's exact evidence; a lexical sibling cannot replace it. A document found
only through lexical search retains lexical passage selection. The existing
weights, requested cutoff, authorization, source revision checks, late provider
approval checks, final publication fences and encoded response budget remain.

Raw node windows retain the existing 64-partition / 4096-candidate budget and
400-document discovery bound. Repeated or ineligible hits cannot silently fill
the visible result set: full windows widen until the eligible document rank is
established, or return the existing typed rate-limit failure. Duplicate nodes
consume scan capacity but cannot multiply a document vote. Invalid raw hits do
not suppress an eligible copy discovered later.

Query article preparation previously returned no roots for a Query selector,
so source-evidence and semantic-record augmentation both skipped its selected
entities. It now uses the exact `selected_member` entity dependencies from the
already prepared context. It verifies the definition, space, entity type,
observed version, inherited subject membership and selection bound. It does
not rerun the query or treat every lineage subject as a selected member. Topic
and Community roots retain their prior meaning. Existing evidence eligibility,
immutable spans, source lineage, security joins and epoch/canonical publication
fences remain authoritative. Article compiler version 18 invalidates receipts
created under the earlier editorial contract.

Incomplete source subject metadata remains Unknown. The complete projection
subject union can include nonmember contributors; exact equality of a public
context and its later article is a qualification-case expectation, not a
universal product rule. The separate S caller repair routes only the two
existing reprocess tools to `/mcp/full` and checks exact original Unknown
inheritance. This product change adds no route, grant, provider allocation or
shared public state type.

Authored verification:

- Actual render/embed/finalize fixtures on both backends check global hybrid
  scores, stale-revision exclusion, hidden policy exclusion, matched-node
  retention, thirteen-chunk multiplicity, and unrelated-corpus stability.
- The existing 401-ineligible-node window fixture now also checks hybrid
  widening, exact eligible document rank and reuse of the governed query vector.
- A unit control checks deterministic document ordering under input permutations.
- Query wiki controls use actual retained source/doc/revision/node/assertion
  rows on both backends, prepare real Query contexts, and publish an article
  through the existing compiler. They require exact source evidence, retained
  event dependencies, Unknown lineage, exclusion of nonmembers and foreign
  policy evidence, and rejection of changed bytes or deleted sources.
- A bounded-root control rejects substituted roots/spaces, invalid dependency
  types, missing versions, absent subject lineage and selection overflow.

The declared vectors and deterministic editorial stub verify mechanics and
boundaries, not semantic model quality. No Cargo command, native test, helper
execution or provider probe was run by G for this checkpoint. Root selects
validation after source, test source and this document are complete.

## Source31 test import correction

The original source30 native run compiled the MCP test target, then failed to
compile the pipeline test target because this new fixture did not import the
domain `Event` type. Source31 adds that explicit import from
`elephant_types::records`. No fixture data, assertions, selected test names,
production Rust, or dependency inputs change. Source30 and its original failed
receipt remain historical evidence; no named test ran in that attempt.

The three Query evidence test bodies still exercise selected-member evidence,
foreign and unbounded membership rejection, and changed or deleted source
rejection on the same storage paths. This source correction has not been
compiled or executed by G. Root owns continuation validation.
