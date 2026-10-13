# Retrieval publication contract

The scoped route is `POST /v1/spaces/{space}/search`; its MCP counterpart is
`search_scoped` in the explicitly selected context space. Surface owns mounting,
authentication, current service grants, readiness, and shared error envelopes.

Requests have `query`, optional `limit` (1-50), `filters.labels` as a map of
string or nonempty string-list values, and `source_revision_constraints` as an
object mapping external source IDs to exact revisions. Each filter key must
match; a scalar means membership and a list means any requested value.
`use_vector: false` explicitly selects provider-free lexical retrieval.
Otherwise query embedding uses `ProviderDispatchContext` and the actual approved
space binding. Entity vector retrieval requires the complete approved policy
identity, not dimension compatibility alone.

Responses always have `results`, `coverage`, and `partial`. Each result contains
`kind` (`claim`, `entity`, or `chunk`), `text`, `evidence`, and `score`. Each
evidence item contains external `source_id`, immutable consumer `revision`,
UTF-8 byte `span: {start,end}`, and the exact `quote`. The optional internal
identifier is explicitly named `internal_doc_revision_id`. No vector is exposed.
Coverage is an object keyed by external source ID, with `requested_revision`
when constrained, `published_revision`, `desired_revision`, and `state`.
States are `pending`, `running`, `complete`, `failed_retryable`, `failed_final`,
`missing`, `stale`, `retracted`, or `erased`. An erasing source is immediately
ineligible and projects erased coverage while its operation tracks cleanup.
Coverage describes publication, independently of query matches. Exact pending
requests return an empty result array with incomplete coverage and `partial`
true; they never substitute the previous published generation.

Output bounds are 50 results, three evidence passages per result, 1024 UTF-8
bytes per result text and quote, and 262144 encoded response bytes. Quote
truncation also updates the returned end offset at a character boundary. Inputs
are bounded to 4096 query bytes, 100 revision constraints, 64 label keys, and 64
values per label. The candidate window is bounded; retrieval is ranked rather
than a promise to enumerate all records.

`storage::read_eligibility` is a shared read projection, not a new publication
state authority. It consumes canonical SourceHead/support records and immutable
SourceDocumentMapping. Exact evidence requires a live authorized Source and
Doc, a valid immutable DocRevision digest, matching complete assertion span,
UTF-8 byte bounds, quote digest, and matching node ancestry. Public scoped
quotes additionally match the original accepted text at the same offsets.
Scoped claims and assertion-supplied entity evidence also require canonical
support ledger ownership of the exact assertion/source/revision. A manual
assertion cannot borrow authority by citing a published passage. Raw retained
assertion reads keep their existing semantics with current lineage checks.
Chunk text is derived from that immutable span, never an unvalidated cached
node text field. A complete source publication is required before its content
can be exposed. Pending desired content cannot borrow an older publication.

Retained HTTP/MCP searches, direct/raw doc reads, entity/relationship context,
assertions, timeline, truth slots, and wiki projections use the same eligibility
rules. Legacy source-first rows without preview heads retain their explicit
live Source/current-render contract. Missing source lineage fails closed.
Intrinsic source-less authority remains readable; a canonical independent
support entry also survives source withdrawal. Existing repository history
selection stays responsible for explicitly requested deleted independent rows.
Source-backed deleted parents and withdrawn source heads remain ineligible.

`with_read_fence(storage, future)` shares observed heads and current Source
envelopes across pages/adapters and rechecks them before returning content.
MCP read dispatch installs it. HTTP read middleware must install it around GET
and scoped/search POST responses; mutating requests use their own transaction
fences. Current source checks happen before asynchronous physical cleanup.
Physical erasure, durable journal/restore barrier, and bounded backup retention
remain integrity/recovery responsibilities.

Regression source binds actual handlers and approved loopback HTTP transport.
The declared-vector ranking fixtures prove path use and profile isolation, not
semantic quality. Qualification owns independent semantic and integrated
publication/erasure oracles. No Cargo compilation or test execution has been
claimed for this source checkpoint. Integration must join source publication,
manifest, public error/auth, provider dispatch, route mounting, and read fences
before validation. Record-level support alone does not establish safe surviving
aggregate fields; that projection requires the integrity-owned support snapshot
contract before erasure acceptance can be claimed.
