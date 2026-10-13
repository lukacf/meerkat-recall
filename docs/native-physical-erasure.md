# Native persistent erasure

Recall finishes native physical erasure through an administrative engine checkpoint,
called outside its logical purge transaction. The source generation/CAS, original
payload-free erasure journal and Recall maintenance barrier remain the authority.
The checkpoint is also called after the terminal operation receipt is written,
before the maintenance guard releases readiness. Startup replays outstanding
journal intents before admitting ordinary work.

The RocksDB implementation rebuilds the exact live native key/value stream into a
fresh engine generation. It preserves all named spaces, indexes, records and
control keys byte for byte. It does not export/reinterpret SQL. RocksDB does not
expose historical version queries in this adapter. Simply compacting tables is
insufficient: historical MANIFEST entries can contain indexed field values, and
diagnostic logs can contain native keys. The entire old engine directory must be
retired, including WAL/archive, SST, blob, MANIFEST, OPTIONS and diagnostic files.

Every native transaction acquires shared admission before creating its snapshot
and retains admission through actual transaction destruction, including cancelled
and committed transactions. The checkpoint drains those readers and owns exclusive
admission through its blocking operation. The replaceable native handle also
covers background WAL flush, metrics and shutdown. A stable sibling OS lock is
held until all engine and transaction owners have closed. It is never unlinked.

The engine checks iterator errors, hashes a length-prefixed ordered stream, then
independently iterates the replacement and compares its row count and digest. It
flushes and closes the replacement, reopens it using the configured native options,
verifies again, closes it, and hands the verified directory to the filesystem
promotion helper. That helper records a durable decision and completes or recovers
the two renames and retirement. The old native handle must be uniquely owned and
closed before promotion. The promoted engine is reopened and verified again before the helper deletes any retired files, and again before
a completed checkpoint receipt is published.

All staging, retired directories, lock and receipt metadata are siblings on the
same owned database volume. For `/data/database.db`, the stable receipt is
`/data/.database.db.recall-physical-checkpoint.json`, with `.next`, `.prepare` and
`.publish` variants used for crash-safe publication. The lock is
`/data/.database.db.recall-native-owner.lock`. The independently retained Recall
journal stays in its original location. Pending metadata contains engine identities,
counts, hashes and file metadata, never source keys, values, source text or secrets.

A pending receipt is durable before irreversible promotion. Startup first recovers
filesystem promotion under stable ownership, then reconciles the pending receipt
against the actual engine ID and exact native stream. A pre-decision abandoned
stage leaves the old engine and no completed receipt; original journal replay
retries erasure. A committed promotion completes retirement and verifies the new
engine. An unknown identity, corrupt receipt or incomplete retirement refuses
readiness. A transport failure, SDK panic or early task exit parks the original Recall maintenance lease and
requires restart because a disconnected server may still own native work.

## Receipt v1

The JSON envelope has exactly `format`, `engine`, `wrapper_version`, `current`,
`current_sha256`, and nullable `previous`. Their constant values are
`recall-rocksdb-physical-checkpoint/v1`, `rocksdb`, and `0.24.0-surreal.5`.

Each checkpoint object has these fields in this serialization order:

1. `sequence` (positive u64)
2. `old_directory`, `current_directory` (objects with `device`, `inode`, both u64)
3. `old_engine_identity`, `engine_identity` (different native IDENTITY values)
4. `rows` (u64), `stream_sha256`
5. `started_unix_ms`, `completed_unix_ms` (u64)
6. nullable `previous_sha256`
7. `original_payload_generations`, `current_payload_generations`
8. `native_snapshots`, `background_errors` (both zero)
9. `completed` (true only in the final receipt)
10. `hard_cancellation_supported` (false)

`current_sha256` hashes UTF-8 compact serde JSON of `current`, preserving the above
struct field order, with no spaces or trailing newline. `previous_sha256` uses the
same encoding of `previous`. A generation object has `name`, `bytes`, then on Unix
`device`, `inode`, all unsigned integers except `name`. Arrays follow lexicographic
relative-path order. Names are validated native engine file names; arbitrary paths,
unknown fields and duplicate names are rejected. Both current and previous objects
are validated before reuse. The envelope is limited to 16 MiB.

The stream SHA-256 starts with the bytes `meerkat-recall/native-live-kv/v1` followed
by a NUL byte. For every native key in byte-order, it hashes an eight-byte big-endian
key length, the key, an eight-byte big-endian value length, and the value. No source
bytes are written to the receipt. This digest verifies a copy; it is not a source
identity or journal authority.

The receipt retains one previous checkpoint, with sequence incremented by one and
`previous.engine_identity == current.old_engine_identity` and matching adjacent
directory identities. The old and new directories within one checkpoint must be
different inodes on the same device; a later checkpoint may reuse an inode that
an earlier checkpoint already retired. This is adjacency, not
an erasure ID: startup can replay multiple intents, and another administrative
checkpoint can intervene. A fresh engine may reuse a filename such as `000009.sst`;
its native engine identity distinguishes the generation. Physical qualification
must inspect the original and final complete owned volumes, including promotion
siblings, and verify actual retirement and independent-data preservation. Neither
a receipt assertion nor a negative canary scan alone proves deletion.

## Bounds and validation

The administrative request accepts one finite timeout from 1 through 300,000 ms;
Recall supplies 60,000 ms. Admission, iteration and background waits consume that
original budget. Blocking native or filesystem I/O cannot be hard-cancelled. A
started owner drains before releasing custody, and expiration returns refusal,
not physical completion. Once filesystem promotion is durably committed, crash
recovery must finish it instead of exposing the stale retired source generation.
External named backups retain their existing bounded expiry and cleanup policy;
physical generation replacement does not waive or reset backup retention.

The native engine controls cover transaction-drop admission, cancelled request
ownership, sticky failure, old MANIFEST/indexed-key and diagnostic-file retirement,
independent blob preservation, two adjacent checkpoints, original owner lifetime,
escaped-handle refusal, and corrupt receipt refusal. Storage controls exercise real
persistent source erasure, the original journal, reopen, and an unknown historical
file that must keep recovery closed. These are authored source controls; execution
and installed physical qualification require the coordinator's separate grant.

The Unix workspace target `native_generation_recovery` includes the exact vendored
filesystem helper source and its unchanged 21 crash controls. It is separate from
the dependency crate's private engine controls, which a workspace test command
does not automatically run. The two persistent storage controls require the
`elephant-storage/rocksdb` feature. The versioned history path is composed separately with the SurrealKV owner;
preserving source-unrelated historical versions requires version-specific
authority and cannot be inferred from this versionless RocksDB replacement
mechanism. Erasure does not enable a blanket history-scrub flag on SQL queries.
The administrative transport and authorization contract is documented in
[native-physical-checkpoint-rpc.md](native-physical-checkpoint-rpc.md).

## Live index workers and caches

The core datastore owns a transaction admission gate shared by ordinary queries,
background index builders and raw native transaction handles. Its read lease is
stored last in `Transactor`, so cancelling or committing a transaction does not
release admission while the native snapshot still exists. Extracting a raw
transactor with `Transaction::inner()` retains the same lease.

The owned checkpoint task first closes index builder and queue-producer admission,
then requests cancellation and waits for every existing builder and daemon to
finish. Workers may still open transactions during this drain. Only then does the
checkpoint acquire exclusive transaction admission. It clears the HNSW map and
datastore cache after the last admitted transaction drops, preventing an old query
from repopulating either cache during generation replacement. The same original
absolute deadline covers worker drain, transaction drain, engine checkpoint and
worker restart. Panic or failure leaves the core datastore closed until restart;
cancelling the RPC waiter does not cancel this owner.

Deferred daemon ownership is reserved before spawning its task. Its completion
guard is already owned if the task is dropped before its first poll, and waiters
register their notification before checking completion. After a successful
checkpoint, deferred and previously unfinished regular indexes restart from
current definitions with a fresh datastore context, without an old query executor
or cached HNSW handle. Ordinary index removal retains its existing asynchronous
cancellation behavior; the physical maintenance path performs the explicit wait.

The internal staging shutdown drains workers and native handles without invoking
cluster-node deletion. RocksDB staging shutdown checks WAL sync, flush, background
completion and exclusive native ownership before closing. These errors propagate
to the generation owner. It shares the original generation deadline.

Five additional storage workspace controls exercise retained cancelled transactions,
escaped raw transactors, cancelled checkpoint callers, cached HNSW queries and
restarted deferred indexing. Three private core controls additionally inspect exact
cache Arc replacement, original owner cancellation and the unpolled daemon race.
All are authored source pending central validation. Removing deferred queue payloads
from a persistent generation additionally requires the shared staging index rebuild;
cache invalidation alone does not certify physical source erasure.

Removed indexes also retain worker custody. Removing a builder transfers its Arc
into the owned retired-worker registry before any suspension point; physical drain
joins both registered and retired workers. The removing query can keep its own
transaction while cancellation proceeds, and an unpolled task cannot escape the
checkpoint. A deterministic private control removes a builder before its first
poll and verifies actual completion before physical admission closes.

Exclusive ordinary transaction admission remains held through worker restart.
A temporary private factory reads only restart definitions and queue positions;
it is never stored in a worker. Restarted workers retain the original factory
and wait for successful owner completion before their first transaction. A
deterministic private control verifies restart metadata can be read while an
ordinary transaction stays blocked, then checks normal admission and a second
checkpoint after release. Existing workspace deferred-index controls exercise
the same active restart path.

## Scoped source history and rebuilt index bytes

Physical erasure uses the private authenticated history endpoint with authorities
constructed from **every** original journal intent. The owner validates stored
journal custody, reads a complete bounded native scope, validates exact current
control rows, and durably acknowledges opaque per-version commitments before the
physical request. One deadline covers inspection, projection, acknowledgement,
copying and the final receipt checkpoint. A stale scope is an explicit failure;
core and SDK admission remain closed for restart/replay. An adapter's read-only
preflight refusal does not grant permission to clear that higher-level barrier.

RocksDB inventories actual current raw keys and values, with explicit versionless
zero version/time markers. The selector proves surviving records and deleted
owners through exact typed identities. Before the first staging write, the copier
omits every selected index prefix, including ordinary scalar entries and worker
queues. The shared index builder reconstructs those indexes from retained current
records. It never first copies a stale source-bearing index key into the new
SST, MANIFEST or diagnostic log. A separate before/after digest verifies all native
keys outside the selected index prefixes, including global bookkeeping.

The memory adapter is configured with native version replacement. Inventory uses
its actual all-versions scan, checks each physical identity occurs only once, and
reports all observed keys; it refuses unexpected historical multiplicity. Selected
index keys are hard-cleared individually under the original exclusive owner. A
private view shares only the actual memory engine with fresh transaction/cache
state and the original capability policy, so the same synchronous index builder
can run while ordinary admission remains closed. No user-facing bypass exists.

The scoped memory scan ends at the actual lexicographic successor of the encoded
database prefix. Appending a sentinel is insufficient because real unknown keys
may have larger suffixes. A workspace regression inserts such a raw native key,
requires inventory to return it, and requires unknown-format erasure refusal.

Persistent staging opens and closes with the operation's original deadline. The
internal staging constructor forwards it through native ownership, recovery and
initialization; it does not obtain a new five-minute budget. Native blocking calls
are checked before and after completion and remain owned while running. The
original datastore capability policy is applied before any analyzer or indexed
field evaluation in the private stage.

The storage client supplies all durable journal intents to this path after the
lifecycle owner has logically purged the complete batch. It binds each authority
to the same inspected native fingerprint and acknowledges its history proof in
the original journal before dispatch. The final completion-receipt checkpoint
reuses the maintenance owner's original deadline; cloning a privileged client
cannot renew it. A native restore supplies its existing five-minute deadline,
while ordinary physical maintenance starts with sixty seconds. Every RPC gets
only the remaining time.

Named native restore binds authenticated imported origins immediately after the
checked import and before guard refresh, quiescence checks or erasure replay.
Its snapshot owner persists the original import floor before database removal.
Raw embedded restore uses the same whole-operation OS ownership lock and compares
its original restore marker before clearing it. An unexposed task retains that
ownership through completion if the requester disappears. Four additional client
controls cover shared deadlines, expired restore before mutation, raw-versus-named
ownership and requester cancellation. These tests are authored, not yet executed.
