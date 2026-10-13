# Managed embedded commit durability

A managed `elephant-db` client with a persistent snapshot destination acknowledges
ordinary mutations only after its transaction delta has been appended and synced
to the local write-ahead log. Shutdown snapshot export is not the durability
boundary. `StorageClient::new_embedded()` and an explicitly absent persistent
destination remain ephemeral library/test configurations.

Every managed `EmbeddedStore` mutation, including typed records, raw vectors,
queue claims, extension/control rows, and maintenance cleanup, uses the same
kernel transaction callback. The kernel rebuilds indexes, invokes the durable
callback under its write lock, and releases visibility only after that callback
succeeds. A callback failure rolls back the entire in-memory transaction and
indexes. An ambiguous filesystem failure also poisons storage admission and
readiness, including previously cloned and maintenance handles, until restart.
A caller receiving such an error must reconcile after recovery: a complete frame
may have reached durable storage even if acknowledgment failed.

## Private live files

For configured destination `PATH`, the stable writer lock protects both `PATH`
and its private sibling directory `PATH.durable/`. The latter contains:

- `identity.json`: version, store UUID, original erasure-journal identity, and
  initialization state.
- `commits.wal`: a checked identity/sequence header followed by complete
  transaction frames. Each frame has a monotonic sequence, a separately checked
  header, a payload checksum, and a commit footer.
- Reserved `checkpoint.next`, `commits.next`, and `identity.next` names exist
  only during atomic publication and are removed before startup becomes ready.

`PATH` holds a versioned private checkpoint with matching store/journal identity,
sequence, and kernel snapshot bytes bound together by its checksum. It is live recovery state, not a
portable user backup. In particular, it preserves Running queue records and
original lease/attempt identity. User-facing named and raw backups still require
quiescence and reject Running work.

On startup, the exclusive writer opens the checkpoint and replays complete,
contiguous WAL frames after its sequence before ordinary admission. Missing
initialized files, foreign journal/store identities, unsupported versions,
sequence gaps, and complete checksum-invalid frames fail closed. An incomplete
final frame is truncated and synced; complete frame headers are checked before
any length is used to classify a torn payload. Recovery never infers an empty
store from a missing initialized checkpoint or log. The existing checked preview
live-snapshot envelope can establish the initial checkpoint; this is not a
migration from arbitrary historical storage.

## Checkpoint and restore ordering

Ordinary commits log transaction deltas rather than serializing the full corpus
per row. At 64 MiB of WAL, the storage import barrier excludes mutations while
capturing one exact kernel snapshot and sequence. Checkpoint publication performs
write, file sync, atomic rename, and directory sync before replacing and syncing
the WAL with a header at that sequence. Startup skips frames already covered by
a published checkpoint, including a crash between checkpoint and WAL replacement.
The current single-frame bound is 256 MiB; larger transactions fail before any
frame is written. This bound is not a document size or source-count limit.

Guarded restore validates the incoming quiescent backup and persists a new
checkpoint sequence boundary and reset WAL before replacing memory. The external
restore marker keeps ordinary admission closed through erasure replay, epoch
rotation, and the final forced checkpoint. A restart with an interrupted restore
retains only an administrative recovery handle until explicit verified resumption.
An old live WAL cannot replay later mutations over the selected backup.

## Physical erasure and retention

The source-erasure reservation is an ordinary durable transaction before the
independent journal records its intent. After physical cleanup, the existing
`persist_restored_state` seam forces a private live checkpoint and WAL replacement
before the erased operation can complete. It works while unrelated queue leases
remain Running. No previous private live checkpoint is retained, and the old
payload-bearing WAL inode is closed after replacement. A forced checkpoint failure
keeps readiness closed; startup finishes replay and the erasure protocol before
serving.

Registered external named backups retain their configured finite deadlines.
Registered raw snapshot exports, their previous generation, and partial export
files have a seven-day bound. These are separate from live checkpoints/WAL, which
must contain no erased payload when physical erasure completes. Cleanup operates
only on owned registered files and reserved private temporaries; it does not
delete foreign caches or arbitrary filesystem paths.

## Qualification

Storage test source covers acknowledged reopen without shutdown/export, exact
Running claims, partial and ambiguous append failures, complete corruption versus
torn suffixes, payload removal from every live file, checkpoint/WAL crash ordering,
concurrent mutation and compaction, guarded restore, and missing/foreign recovery
identity. Independent scenario tests use a real child process and SIGKILL after
repository acknowledgment; they do not manufacture backing snapshots before the
kill. Source authorship and static formatting are not runtime qualification.
Compilation and execution are coordinated from the integrated source checkpoint.

Checked native queries preserve the actual failing statement instead of an
older statement's generic transaction-rollback marker. Both bound and unbound
queries select the first substantive error in statement order; a marker remains
an error if no fuller cause is available. Existing conflict retry and storage
error classification apply to that cause. Authored regressions exercise a thrown
transaction error with rollback and preserve successful statement positions.
## Native transaction overlap test seam

Storage unit tests have a private constructor for an isolated native memory
engine that permits exactly one explicit `127.0.0.1` address and nonzero port.
It requires an explicit journal directory and retains the ordinary durability,
reference and recovery initialization. Remote stores, snapshots, other loopback
addresses, IPv6, wildcard addresses and port zero are rejected before opening
the journal. Production constructors retain their original network capability
defaults and execution deadlines.

The independent concurrency oracle owns the bound ephemeral listener, bounded
in-transaction HTTP acknowledgement and release, competing actual writer,
cancellation and cleanup. A source test also requires other ports and ordinary
clients to receive a capability refusal rather than a connection timeout. This
seam requires the pinned SurrealDB HTTP feature only as a test dependency; it is
not a product provider or runtime network configuration.
