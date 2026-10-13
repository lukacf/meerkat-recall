# Snapshot durability and erasure recovery

Both `elephant-db` and `surreal-db` use the storage snapshot library. A named
backup is an immutable directory containing `manifest.json` and `data.snapshot`.
The manifest identifies format version 1, backend, snapshot UUID, erasure journal
UUID, name, creation/expiry time, byte length and SHA-256. Verification performs
no writes and rejects malformed, altered or expired bundles. Verification is an
integrity check, not authentication of an untrusted backup producer.

`meerkat-recall snapshot inspect --input /absolute/named-backup --json` returns a
bounded physical inventory of a local verified bundle. It opens no database,
contacts no service, reads no signing key or recovery journal, and does not
restore or execute SQL. Its default JSON contains the manifest, table/row counts,
typed physical identities, per-row hashes and typed references. Original record
values and schema SQL are omitted. The plain text mode reports completion without
printing payloads.

An operator can request original typed records and schema statements with
`--json --include-records`. Treat that output as private backup data. For example,
capture it in a newly created private file rather than a shared log:

```sh
umask 077
set -C
meerkat-recall snapshot inspect --input /absolute/named-backup \
  --json --include-records > /absolute/new-private-inspection.json
```

Inspection refuses changed/expired manifests, non-regular inputs, oversized or
unsupported values, duplicate identities and incomplete decoding. It never
reports a partial inventory as successful. The storage decoder has fixed bounds;
the CLI exposes no limit-widening option. The compact JSON retains the report's
byte bound. A failed invocation returns nonzero and emits no successful JSON
inventory; shell redirection may still leave an empty private file.

For escaped or repeated long payloads, add `--compact-records` to both
`--json --include-records`. This explicit option produces
`recall-snapshot-inspection-v2`; the default remains v1. Every original row,
identity, reference and schema statement remains present. Strings and byte values
of at least 1,024 bytes inside record payloads use typed `string_ref` or `bytes_ref`
values pointing into a shared `blobs` dictionary. Each lowercase SHA-256 key
addresses the exact original bytes; its value contains only `bytes` (original
length) and `base64` (standard alphabet, no padding). Identical string and byte
values can share an entry while their reference tags preserve the original type.
Native record identities, including composite keys, retain their v1 shape.

Row hashes still cover the original tagged representation before compaction.
Consumers must check dictionary shape, canonical encoding, byte length and hash,
UTF-8 for string references, complete reference coverage and absence of unused
entries, then verify each expanded row's original hash. They must retain the
original report and process one expanded row at a time. Inspection keeps the
64 MiB input, 32 MiB report and existing parser/node/row/reference limits. Both
unique decoded dictionary bytes and the sum of original string/byte leaf lengths
across all records are capped at 64 MiB; repeated occurrences, inline values and
values inside composite record IDs each count. A source whose actual exported
inventory exceeds these limits returns an explicit incomplete error. Compaction
does not by itself qualify a maximum-size Graph workload or physical erasure.

This inventory describes the supplied bundle only. Current live data, retained
older backups, previous live checkpoints, media files, cold rows and the original
independent journal require their own applicable physical evidence. A source UUID
can legitimately remain in a payload-free control tombstone; substring absence
or a successful export verification alone does not establish physical erasure.

Export pauses admission and scheduling and drains producer/publication operations
before taking an exclusive storage barrier. The quiesce deadline defaults to 30
seconds and is bounded at 60 seconds. A work item still marked Running in any
space causes a conflict; export never relabels active attempts. Repository
queries and embedded store handles share the barrier. Whole request/worker
operations use `StorageClient::with_activity`, including preparation and final
publication. An abandoned SurrealDB caller retains storage admission until the
SDK request actually returns. Existing calls drain; new calls receive an immediate maintenance refusal.
Readiness becomes false before the drain begins. Timeout does not produce a backup receipt.

Embedded restore validates a replacement kernel before swapping its complete
state. SurrealDB restore removes the destination database and imports the full
verified schema/data, checking all statement results. It is a durable resumable
replacement: a synced external marker precedes destruction, and any failure or
process crash prevents normal reads, writes and readiness. A subsequent explicit
verified restore restarts full replacement; it never merges a partial dataset.
The marker clears only after erasure replay, canonical epoch rotation, required
local persistence and retained-backup cleanup. No restored extra rows survive.
Backups must be trusted administrative artifacts; SurrealDB bundles contain SQL.

The independently retained journal records only opaque source-incarnation IDs,
space IDs, intent UUIDs and timestamps. An intent is synced after a durable Erasing fence accepts the source
precondition and before any live purge. A read-only precheck is insufficient. Replay is idempotent and precedes
startup readiness and every restored readiness transition. After intent persistence, physical purge takes an exclusive activity barrier so
old rendering, extraction and embedding tasks finish before their payloads are
removed. The accepted erasure continues under owned storage admission if its
HTTP caller disconnects. Failed live cleanup blocks storage until replay. An explicit successor source uses a new physical
incarnation so replay of the old incarnation cannot delete corrected content.
The journal also stores payload-free restore markers and owned-backup retention
metadata. It is never included in a data backup or replaced during restore.
A backup's journal UUID must match the separately retained journal; supplying a
new empty journal is refused. SurrealDB also retains an immutable journal UUID
anchor in live data; startup compares it without changing schema. The marker
only detects substitution; erasure history remains outside restored data.

`ELEPHANT_DURABILITY_PATH` selects the exact journal directory for either backend.
Without it, local stores use a stable sibling directory; remote stores use a
store-specific directory under `.elephant/durability`. The journal cannot be
nested inside a replaceable store. Container deployments should put it on the
separate recovery volume. Physical volume recovery must preserve this journal
independently; restoring the journal to an earlier point defeats erasure history.
Remote deployments share that journal volume and its lifetime writer lock and
must exclude independent database writers. Direct external administrative writes
are outside the application's admission barrier.

Backup retention defaults to 7 days, with a hard maximum of 30 days. Expiry
immediately refuses restore. Owned backup payloads, manifests and incomplete
export generations are physically deleted on startup and every 60 seconds while
the process is running. Cleanup failure prevents readiness. Expired files on a
stopped host are removed before it becomes ready again; continuous wall-clock
deletion while stopped requires the host's retention scheduler. The registry
never recursively deletes arbitrary operator directories. Copies made outside
this ownership registry require equivalent operator retention. Provider-side
retention remains governed by the approved processor's separate contract.

Managed embedded storage with a persistent destination acknowledges ordinary
mutations only after its transaction delta is appended and synchronized to the
local write-ahead log. Startup replays that log before admission; shutdown export
is not the commit durability boundary. Live checkpoint publication uses an atomic
same-directory write, file sync, rename and directory sync with a checksum and
store/journal identity. Physical erasure replaces the live checkpoint and WAL
before reporting completion, retaining no previous payload-bearing live checkpoint.
Registered raw snapshot exports and their previous generations are separate backup
artifacts with a seven-day retention bound. Current live data does not expire.
Runtime configuration supplies one resolved load/save path; mismatched paths are
refused. Explicitly ephemeral library/test stores without a persistent destination
remain volatile. See [managed embedded commit durability](embedded-durable-commits.md).

Fresh SurrealDB initialization checks every statement and commits each schema
group together with its checksum receipt. Startup verifies complete, contiguous,
matching receipts without applying schema. Use explicit initialization/migrate
for a fresh database or interrupted initialization. An old populated database
without verified receipts is refused; no general legacy conversion is promised.

Signing keys and service-token secrets are not part of database snapshots.
Operators must retain signing-key material independently or issue replacement
tokens under the recovered signing authority. Backup manifests and evidence
must never contain signing secrets.

The retained bounded backend-transfer command uses a distinct incomplete-transfer
marker before writing schema or rows. Its owned storage task keeps the exclusive
barrier through the command's preservation and inventory reconciliation. Failure
or crash leaves the target unready; successful schema initialization alone cannot
make a partial copy serve. This marker is not a restorable snapshot marker.
Only explicit discard of the partial target, matching the original source digest,
clears a failed transfer; retry starts again on the empty target after preflight.
The source snapshot is never modified. Preview snapshots with erasure-aware
control state use the named restore procedure and their retained journal, rather
than the older backend-transfer path.
