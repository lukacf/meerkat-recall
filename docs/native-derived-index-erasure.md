# Derived indexes during native source erasure

The physical-generation owner rebuilds affected search and vector indexes in an
unpublished staging datastore. The source-history authority selects the record
versions that survive and excludes every old aggregate or builder-internal
version, including orphan keys and soft-deleted historical values. Index
reconstruction does not authorize removal of records or publish a generation.

In the retained SurrealDB 2.6.5 implementation, direct record reads and table
scans pass `SELECT VERSION` to the record store. Search, HNSW and ordinary index
iterators read current index keys; records fetched through those iterators also
use the current version. Versioned exports enumerate record keys, not historical
aggregate index pages. Reconstruction preserves this existing distinction. It
does not add historical search semantics or discard unrelated record history.

The native inventory identifies exact namespace, database, table and index
names. Reconstruction resolves each current index definition from that verified
inventory and compares it again with the private stage before writing. Removed
definitions need no new index. Scalar index builder queues are removed by the
source-history selector while independently authorized ordinary index entries
retain their original versions. Reconstruction soft-deletes only the current
scalar entries and computes their current replacements from retained records.
This removes stale entries from deferred field changes while preserving older
versions and enforcing uniqueness among the actual retained current records.

The deferred builder also writes an initial marker at the exact bare native
`+index\0` key. The history decoder round-trips that full prefix and classifies
only that exact key as internal builder state: it must be removed and its index
reconstructed. A truncated prefix, unknown suffix or malformed tagged internal
key still refuses the checkpoint.

Full-text reconstruction writes one table-local `!bs{index}\0` state key outside
the usual `+index\0` prefix. Protected-history verification permits that exact
key only for a selected, retained current Search definition. The shared
`rebuild_extra_keys` contract supplies complete native keys; consumers use set
membership, never prefix matching. Scalar, vector, removed or unrelated indexes,
foreign namespaces/databases/tables, malformed suffixes, other table metadata and
global counters receive no additional exception. Ordinary historical index
versions remain protected under the existing version boundary.

The reconstruction transaction requires every old aggregate and builder-internal
key to be absent across the native version stream. It permits only exact typed
ordinary index entries from the source-authorized selection, including historical
scalar entries under a now-aggregate definition. Native ordinary entries use the
`*` key class; aggregate and builder pages use the disjoint `!` key class. Current
aggregate reads cannot consume retained legacy scalar entries. Unknown key shapes
always fail. Current-only absence is insufficient
on SurrealKV. The same reconstruction serves RocksDB, where the native engine
reports that versioned scans are unsupported and only current key/value entries
exist; this fallback is restricted to the actual RocksDB datastore.
It evaluates the retained current records using the same `Document` field
evaluation and `IndexOperation` implementations as `REBUILD INDEX`, including
the configured analyzer, distance metric and index parameters. Search, HNSW,
M-tree and count indexes are supported. Current index definitions are preserved;
deferred indexes receive their completed initial-build marker after synchronous
reconstruction. No background builder or deferred daemon starts in the stage.

Each reconstruction uses fresh index caches and the staging datastore's
capabilities. All selected definitions are checked before any writes, and all
reconstructed indexes commit together. The original checkpoint deadline applies
through target discovery, inventory verification, scanning, field evaluation and
commit; runtime callers pass it to `collect_targets_with_deadline`. Cancellation drains before the
stage can be discarded; a late or failed commit acknowledgement cannot qualify
the generation for publication.

The generation owner remains responsible for stopping live builders, draining
original transactions, invalidating live index caches under exclusive ownership,
closing the staging engine, checking the unchanged retained record/version
stream, verifying the completed native generation after reopening, and retiring
the original files and bounded backups. A reconstructed index is not evidence
that those physical-erasure steps have completed.

The source controls exercise persistent SurrealKV reconstruction, unrelated
historical records, current indexed queries with `VERSION`, full-text/vector/count
results after reopen, refusal of orphan and soft-deleted index history, changed
definitions, duplicate targets, expired deadlines, retained deferred schema,
scalar history preservation and rollback on uniqueness conflicts, and the same
search/vector reconstruction on RocksDB.
They are authored source; execution results must be reported separately.

The central R34 run reached these workspace controls and exposed invalid fixture
syntax before their physical-checkpoint assertions: SurrealDB 2.6.5 accepts
historical `INSERT ... VERSION`, but has no `UPSERT ... VERSION` clause, and it
rejects `UNIQUE DEFER`. The corrected fixture inserts complete original records
at the same historical timestamps. Its scalar scenario uses an ordinary deferred
index and a separate unique index over the same records, reproduces stale and
delayed entries in both, and requires both to preserve independent history and
return the original expected current records. Orphan builder markers make both
reconstruction obligations. The separate uniqueness-conflict refusal remains
unchanged. These corrections require a subsequent central run; source review
does not turn the failed R34 controls into passing execution evidence.

The workspace storage library also contains six controls behind the explicit
`surrealkv` and `rocksdb` features, three per engine. They call public native
inventory and physical-history checkpoint methods with the production HMAC
membership authority. They exercise mixed-source versions, independent corpus
history, cached search/vector/count results and reopen, deferred scalar rename
and deletion, scalar-to-search transitions, and an actual uniqueness failure that
must keep query admission closed. Native transaction fixtures represent persisted
index lag; they do not replace index construction. The separate engine oracle
owns physical-file canary proof. These six and the eight private-core controls
are authored, unexecuted source until a separately recorded validation run.

The scalar workspace scenario also reproduces a deferred A contribution whose
native index entry is committed after an independent B document has replaced A.
It requires every old A field-key version to disappear while B's document and
independent ordinary history remain. Index-builder transactions do not carry the
original document version, so timestamp proximity alone cannot prove scalar
entry ownership. This regression depends on the source selector validating the
actual retained field evidence; a refused or unfinished checkpoint is not a
passing physical-erasure result.

Historical scalar evidence uses `recall_scalar_projection::scalar_record_keys`.
It accepts an independently retained native document and compatible inspected
scalar definition, runs the native `Document::build_opt_values` evaluator, and
uses the same native `Indexable` Cartesian/array/flatten and unique-null rules to
encode full ordinary keys. A field's complete encoded key must match; an index
commit timestamp is insufficient. The selector owns record decisions, historical
schema compatibility, and the final retained-version decision.

Projection has no datastore transaction. It proves that accessed values are
literal and paths remain local before evaluation, then disables all functions
and networking. Dynamic selected values, record dereferences and unknown path
parts refuse evidence; unrelated unaccessed dynamic fields are not evaluated.
Native depth, input/output size, iteration bounds and the original checkpoint
deadline apply. The actual staged rebuild separately inherits the original live
capabilities. The public negative reconstruction scenarios also require a denied
analyzer function to fail before any successful physical completion.

Six additional private-core source controls cover exact Cartesian native keys,
unique null/empty behavior, flattening, local nested arrays and compound IDs,
terminal record references versus dereferences, dynamic selected versus unrelated
fields, and scope/kind/deadline refusal. Together with the original eight private
controls and six workspace tests, twenty test functions are authored; none of
these native controls have been executed in this source checkpoint.

The local scalar preflight preserves native implicit-array mapping shape: it
consumes a field (and a following wildcard) per element, collects the mapped
array, and applies the remaining suffix to that array. It checks the resulting
selected values before invoking the native index evaluator. The nested-array
regression distinguishes this from applying the whole path per original
element: both selected siblings produce keys, a selected remote record or Future
refuses proof, and an unselected mapped sibling is never evaluated. These
controls are authored and have not been executed.
The same control distinguishes direct `items[0].id.part` access to a complex ID
from implicit `items.id.part`: the latter maps IDs into record references before
the suffix and therefore refuses dereference. The native evaluator body is
unchanged; the retained object-field wrapper also falls back before arrays.

The existing public SurrealKV scalar workspace scenario also runs eight focused
preflight cases through actual SQL-created indexes, inspected native entry
bytes, a stored revision, and the public physical-history checkpoint. Both
implicit field mapping and field-plus-wildcard mapping preserve the two selected
keys while ignoring an unselected dynamic sibling. Selected remote records,
selected Futures, and mapped complex record IDs must fail with the projector's
specific preflight error and leave admission closed; direct complex-ID access
remains valid. Successful cases preserve the independent named-space document
and all original scalar versions through close/reopen. This folds the exact
counterexamples into the existing six workspace test names without exposing a
private helper. They remain unexecuted until the central validation run.

The later central R35 run executed all six workspace physical-index scenarios.
It exposed the bare deferred marker codec refusal and the full-text state key
missing from protected-history verification. The repairs above add a public
codec/drop/rebuild regression and private exact-key, scope, index-kind and
malformed-key controls. Existing physical checkpoint assertions remain intact.
These new repairs and controls are source-complete but require a new central
validation run; the earlier run remains failed. Historical statements above
about unexecuted controls describe their original source checkpoints.

The public persistent-engine fixtures use native `bootstrap` on every initial
open and reopen. This registers the datastore's cluster node and restarts any
retained deferred indexes, matching normal startup. Native `shutdown` first
removes that registered node; constructing a datastore alone does not register
it. Three R37 scenarios reached strict shutdown after their checkpoint and query
assertions, then failed because the fixture had omitted bootstrap. The startup
correction preserves every shutdown, reopen, history and query assertion; it
does not suppress a shutdown error or establish a passing runtime result.
