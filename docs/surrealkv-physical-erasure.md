# SurrealKV physical generation checkpoint

The paired server retains its upstream SurrealKV and versioned SurrealKV engines.
The Recall workspace's ordinary local feature selection still enables memory and
optional RocksDB; recognizing a URL does not enable another storage feature.

The SurrealKV adapter acquires a stable sibling OS owner lock before recovery or
opening the engine. Every native transaction holds a read admission lease until
its native snapshot is dropped. A commit worker owns a cloned lease and explicitly
drops its native transaction before releasing it, including when its caller is
cancelled. Physical maintenance drains those leases, takes the write lease and
retains it in an owned task through blocking native work. Failure after the first generation mutation poisons admission
until startup recovery. Read-only preparation refusals leave the original store
available for a new inspection. Shutdown errors propagate.

The vendor checkpoint writer serializes the exact live index version stream into
an empty staged generation. Original keys, values, transaction versions, timestamps
and surviving soft tombstones remain. It does not copy original log, manifest,
repair or compaction files. A checked durable clock prevents transaction version
reuse after the highest-version key or every key is removed. A copy alone cannot
establish erasure: the adapter independently reopens and hashes that stream, closes
all old handles, and calls the shared native generation helper. The helper validates
the final renamed generation before deleting the entire original directory and
synchronizing its parent. No compaction-success shortcut is used.

The engine certificate is the adjacent
`.<database basename>.recall-physical-checkpoint.json`. Its pending sibling uses
`.next`. The strict `recall-surrealkv-physical-checkpoint/v1` envelope identifies
engine `surrealkv`, wrapper `0.9.3`, current and immediate previous checkpoints,
old/new UUID engine identities, old/new directory device/inode, complete original
and current file inventories, surviving version count/hash and `high_water`.
Its hash domain is `recall-surrealkv-surviving-versions-v1` followed by NUL,
length-delimited native record encodings, the big-endian high-water clock and
record count. A completed certificate follows actual retirement and native reopen.
Startup recovers the original directory decision before completing a pending
certificate or admitting transactions. The certificate proves a physical copy and
retirement; source-scoped policy still has to select the correct historical values.

The version writer takes an immutable per-version decision from the original
source-erasure authority. `Keep` preserves the native encoding. `Tombstone`
removes the value at the original transaction version and timestamp while keeping
its absence boundary; simply omitting a mixed-source version could expose an older
independent version at the erased timestamp. `Drop` omits a version only after
policy proves no independent history needs that key. The raw inventory includes
soft-deleted keys and marks their exact latest version. Ordinary transaction writes
have no history-clearing flag.

The active history command captures every native version under the exact encoded
namespace/database prefix, including records absent from current reads. The shared
snapshot codec limits the complete capture to one million versions and 64 MiB;
overflow returns an error, never a truncated success. The canonical owner derives
causal decisions using the original native transaction IDs and its existing domain
converters, durably records opaque commitments in the original erasure journal,
and submits bound authorities. The physical worker captures the same scope again
under exclusive admission. Any scoped fingerprint change refuses before directory
mutation, allowing a fresh inspection and preparation. Other namespaces and global
heartbeats do not invalidate this scoped comparison. Engine preflight refusal
leaves its native generation healthy, but the core and SDK owner barriers remain
sticky on failure and require the original restart/replay path; no transport error
string alone authorizes reopening those higher-level barriers.

The worker consumes the shared immutable history selection, verifies the selected
native copy, and reconstructs affected indexes in an unpublished core datastore.
The shared index helper resolves original current definitions and uses native index
operations; it owns aggregate reconstruction and scalar deferred reconciliation.
The core owner supplies its original capability policy by value to the owned
native worker. Staging installs that policy before any index expression or analyzer
function runs. No default capability policy substitutes for the original one.
The staging datastore closes through the internal native-only shutdown hook, so
closing it cannot mutate cluster membership. The filesystem helper independently
locks and removes the one exact staging owner-lock file before promotion/recovery.

Before and after reconstruction, an independent stream digest compares every
unaffected version. Original ordinary-index versions remain in that comparison;
only newly appended reconciliation entries and rebuilt internal index nodes are
excluded. All record history, unrelated counters, definitions and global state
must remain byte-exact. The final native clock cannot move backward. The fresh
final stream is then sealed and independently reopened before old-directory
retirement. The core owner separately drains builders and query transactions and
clears live caches under its gate; native file replacement alone cannot clear an
old in-memory HNSW tree.

The source supports the shared selector and reconstruction contracts, but runtime
and physical-erasure qualification remain outstanding. The selector's explicit
refusals, such as unsupported source-aware change-feed history, propagate; the
adapter never substitutes an unfiltered copy after an authority error. Restored
causal evidence and nonce-only copied derived values require the canonical
projection owner's precise proof, not a content-only inference.

The certificate accepts only `recall-engine.identity`, `recall-checkpoint.clock`,
`clog/<20 decimal digits>.clog` and `manifest/<20 decimal digits>.manifest`.
Unknown files or directories fail before promotion and cannot be copied into
receipt filenames. Engine identities are initialized through a fixed temporary
file, synced, atomically renamed and directory-synced; a torn authoritative
identity is never silently replaced. Native transactions and detached commit
workers retain the stable OS owner through their last native handle, including
unwinding. One original deadline covers staging, validation, promotion and receipt
completion. Startup similarly uses one recovery deadline.

All new native checkpoint, history-copy and custody tests are authored source
only. Private vendor tests are not automatically run by workspace tests. The
coordinator must select their actual execution scope before qualification.

The internal staging opener accepts the original absolute deadline; it cannot
start a fresh startup allowance. Normal startup keeps its own bounded recovery
window. A deadline already expired refuses before creating any staging files;
blocking native I/O remains explicitly non-cancellable and is checked again before
admission.

Plain SurrealKV selects the versionless policy only from the actual immutable
native Store setting, while retaining genuine native version IDs and timestamps
in inspection. It omits selected current index prefixes before writing a fresh
stage and reconstructs them from surviving current records. Versioned SurrealKV
uses the strict historical selector and preserves all independently supported old
ordinary-index versions. An all-tombstone key can disappear only when the shared
selector proves its exact original membership and no independent value survives.
