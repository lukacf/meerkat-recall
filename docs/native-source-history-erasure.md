# Native source history erasure

Native erasure evaluates each retained record version under the original
namespace, database, named space, journal identity, private HMAC key and durable
membership. The physical generation command carries this authority as a strict
native object. It is not a SQL option, user binding or ordinary query function.
The transport must redact the entire authority; receipts never contain its key.

Inspection runs after the logical purge while the original maintenance owner
remains held. It returns every native version in the selected namespace/database,
including hidden deleted keys and index state. A sorted scoped fingerprint binds
the exact key, native transaction version, timestamp, value, tombstone and current
marker. It deliberately excludes unrelated global heartbeat state. Inspection
refuses more than one million versions or the 64 MiB raw budget; the RPC applies
its 64 MiB encoded budget as well. Neither path truncates a successful response.
Native versions and timestamps use eight-byte big-endian transport values so the
full unsigned range survives without floating-point or signed-integer coercion.

The original lifecycle owner projects causal evidence with the existing domain
converters and commits opaque exact-version HMACs to the durable journal before
physical replacement. Content equality alone is insufficient: an independent
earlier B version may have precisely the same content as a later A version.
Current minimal-control exceptions and known historical payload-free system
metadata are also exact inspected-version commitments, recomputed from the full
original-writer schema and identity validation for each inspection. They are not
durable permissions to retain arbitrary past or future values. Before the
copier applies the immutable selection, a fresh complete scoped inventory must
match the original fingerprint. Old journal commitments whose original versions
are already absent cannot authorize deletion of another version.

`recall_history` decodes native record keys and revision-encoded values. It uses
the existing typed reference normalizer, including encoded reference objects and
composite IDs. It does not infer ownership from UUID substrings. A foreign-space
version remains independent even when a later version of that physical identity
belongs to the erased space. Missing space or unknown native key classes require
an explicit caller policy; neither means successful independent preservation.
Known metadata keys must round-trip through their exact native codecs. Unknown
database or table suffixes fail; they cannot inherit metadata retention.

The history copier enumerates every native version, including keys whose current
state is deleted. For a shared record, it retains independent versions exactly.
A contaminated historical value requires a tombstone at its original version
and timestamp when earlier independent versions survive. Merely omitting that
value could expose an earlier value at the erased version's timestamp. Complete
identity removal is valid only after all versions demonstrate that no independent
value survives. Current source-bearing payload is an error after the logical
purge. An exact current minimal-control verdict from the lifecycle owner is the
only exception; a table name is not such a verdict.

The decoder also exposes typed ordinary index fields and owner identities, graph
endpoints, reference endpoints and native unique-index owner values. Derived
entries need exact independent historical evidence. Scalar selection first
classifies every record version, then projects native scalar keys with the actual
field evaluator and native Cartesian/array/unique-key machinery. The inspected
scalar definition and document value must have overlapping native version
intervals before the derived write. Exact encoded key equality with a retained
record supplies retention; an erased-only witness masks/removes the entry.
A later independent value cannot justify an earlier index version even when its
encoded tuple is identical. Missing schema or unproven tuples refuse. Deferred
index writes can commit after
a newer document: a latest-owner temporal join cannot establish their origin.
Dynamic dependencies cannot be historical proof. Graph and reference keys retain
their separate typed owner/version joins. Full-text and vector index internals
are explicitly distinguished because their aggregated nodes need the native
index reconstruction mechanism. They are not revision-encoded document values.

Deferred index queue/state versions are also removed. Every removed internal
prefix creates a mandatory staging reconciliation obligation, including ordinary
scalar indexes. Scalar reconciliation retains original independent scalar index
history, removes current scalar entries and reinserts native current values in one
staging transaction to ensure current coverage. Aggregate indexes rebuild from current retained records. The generation
cannot be promoted until the native reconstruction owner satisfies these
obligations and verifies unaffected records and histories remain exact.

Versionless engines use a distinct preparation entrypoint with current-only
versions. RocksDB has zero version/timestamp markers; a nonversioned SurrealKV
store retains its actual internal transaction clock. Every decoded ordinary index
is omitted before any staging write and added to the same reconstruction
obligations. Writing stale entries and then deleting them would leave their bytes
in the new WAL or file metadata. True versioned scalar history keeps the separate
per-version policy above. An already deleted graph/reference owner can then be recognized by its exact original journal identity token. The historical
selector refuses that fallback because missing owner history cannot prove that
independent earlier versions are safe to remove. Native change-feed entries still
require a dedicated source-aware codec; encountering them refuses this component's
selection. No current Recall migration enables a change feed.

Backup origin uses separate HMAC domains for exact native record content and for
the origin owner's canonical sidecar bytes. Both bind the original journal,
namespace and database without exporting the private key. A third separate domain
binds a canonical `source:<UUID>` identity and its named space for the causal map;
an entity ID, noncanonical UUID or empty space is rejected. The complete imported
origin union across namespaces and databases has its own journal-only MAC domain.
Its owner holds the original journal writer lock across authenticated read, union,
file fsync and durable acknowledgement, preserving other scopes. A content lookup is
not a historical erasure proof. Before any erasure intent exists, the explicit
backup-inspection constructor can compute version commitments using the same
original journal context; its empty membership cannot bind a physical authority
or pass the physical RPC decoder. For imported data, the restore owner must authenticate the complete
sidecar, backup digest and actual imported correspondence before using it.

These codecs and per-version predicates do not independently establish complete
erasure. The active generation integration must combine them with the existing
causal support/decision authority, complete retained-history scan, derived-index
handling, durable retirement of the old generation, and original restore barrier.
Authored private tests and the storage `native_history_selection` integration
target cover the codec and predicate contract with controlled native-version
fixtures. They do not claim a live engine history run. Installed erasure
acceptance requires the separately captured physical and logical evidence.

The full-text engine writes its global `Bs` state at the table-local
`!bs{index}` suffix. This differs from the `+{index}!tag` layout used by the
other index internals. The history decoder validates the exact `Bs` constructor
round trip and classifies it as aggregate index state, so every captured version
is omitted from the staging copy and the existing native rebuild is mandatory.
Unknown table suffixes, malformed keys and a relocated `+{index}!bs` key still
refuse selection. A storage integration regression creates a real memory-backed
full-text index and requires its actual emitted global-state key to be dropped,
its rebuild obligation to be present, and its independent document to survive.
This authored regression is not an executed qualification result.
