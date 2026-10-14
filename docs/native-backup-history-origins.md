# Native backup history origins

Native SurrealDB export preserves document values but assigns new transaction
versions during import. A source erasure cannot use those imported transaction
coincidences as original causal proof. Named native backups therefore include
`history-origin.json` alongside `data.snapshot` and `manifest.json`. Embedded
`elephant-db` backups keep their existing format.

The existing source decision authority supplies each exported current record's
opaque invalidating-source tokens. Export uses its complete native-history
projection **and** the original live inverse planner. This preserves current
field-level independent/contact support, including on versionless Mem/Rocks.
An empty token set means the authority evaluated that record; omitted coverage
is an error. The original private `source_erasure_membership` table remains
excluded from both the export and its sidecar.

The caller holds the original maintenance reservation throughout capture,
projection, export and a second complete native inventory. The before/after
scoped history fingerprints must match. A single 300-second native proof budget
covers these operations; the existing admission-drain budget is separate.
No new operation receipt contains source payload or private key material.

## Exact files and custody

Both new files use compact canonical UTF-8 JSON. The writer's struct order is
part of canonicalization. Unknown fields, duplicate tokens, alternative numeric
spellings, extra whitespace, invalid lowercase 64-hex commitments, duplicate or
unsorted records, noncanonical UUIDs, oversized inputs, aliases, hardlinks and
nonprivate Unix files are refused. Each file is bounded to 64 MiB, the total row
count to 1,000,000, and the total causal-token count to 1,000,000. Imported state
also allows at most 1024 distinct namespace/database scopes. Exceeding a bound
refuses the operation without silently dropping proof.

`history-origin.json` has fields `payload`, `authentication`. Its payload fields,
in order, are:

1. `kind`: `meerkat-recall-backup-history-origin/v1`.
2. `journal_identity`, `key_fingerprint`: the original independent journal.
3. `namespace`, `database`: the exact native scope.
4. `snapshot_id`, `snapshot_sha256`: the original manifest identity and data hash.
5. `manifest_sha256`: SHA-256 of compact serialization of the entire original
   `SnapshotManifest`, including its finite retention timestamps.
6. `rows`: sorted by opaque `content` commitment.

Each origin row has `content`, `origin_mode`, `original_version`,
`original_timestamp`, `causal_sources`, in that order. `content` authenticates the
exact native key and full native value using the original key, journal and scope.
`origin_mode` is `native_versions` or `versionless_current`; the latter requires
both original numeric fields to be zero. `causal_sources` is a sorted unique set
of opaque source commitments, never source IDs or payload. Native version and
timestamp fields are full-width unsigned integers, not signed JSON surrogates.

The sidecar authentication uses the codec owner's
`backup_origin_authenticator`; record lookup uses
`backup_origin_content_commitment`, and causal source tokens use
`backup_origin_source_commitment`. These domains are distinct from one another,
from erasure version commitments, and from the whole-journal authenticator.
No fake native version or synthetic database name is used as a crypto domain.

`imported-history-origins.json` lives in the original independent journal. Its
outer fields are also `payload`, `authentication`. Payload fields are `kind`
(`meerkat-recall-imported-history-origin/v1`), `journal_identity`,
`key_fingerprint`, `scopes`. Scopes are sorted by `(namespace, database)` and each
has `namespace`, `database`, `pending_import`, `rows`, in that order. Imported
rows have `origin` (the complete origin row above), `imported_version`,
`imported_timestamp`, `snapshot_id`, `snapshot_sha256`; they are sorted uniquely
by `(origin.content, imported_version, imported_timestamp)`. The separate
`backup_origin_journal_authenticator` signs the entire canonical multiscope
payload, so one scope cannot be silently removed or substituted.

`pending_import` is null after complete binding, or has `snapshot_id`,
`snapshot_sha256`, `origin_sha256`, `max_preimport_version`. `origin_sha256` hashes
the exact compact authenticated backup payload. The version boundary is captured
from the complete native inventory before the first destructive import and is
never advanced during retries of that restore.

## Restore and crash recovery

Restore verifies the data/manifest, sidecar shape, original journal/key, exact
scope and HMAC **before** destructive replacement. Offline `snapshot verify`
checks the sidecar structure and manifest binding but cannot authenticate its
HMAC without the original journal. Old native artifacts missing required proof
are refused; the tool does not manufacture provenance or migrate them implicitly.

The existing OS ownership primitive holds the journal's stable
`restore-operation.writer.lock` for the entire named restore. Independent handles
cannot run overlapping destructive restores even when their marker matches.
Marker creation compares the original snapshot identity under the original
journal writer lock; checked completion cannot remove another operation's
marker. The raw embedded import entry point uses the same ownership and checked
completion contract.

The existing durable restore marker precedes replacement. The signed original
journal then records the pre-import version boundary and its mandatory index
acknowledgement before any import. After the checked native import, the client
binds the actual imported versions **before** reference-guard refresh, journal
identity initialization, erasure replay or readiness. The full current native
key/value set must match the authenticated backup. Existing equal rows at or
below the original boundary do not become imported source authority.

A crash after import but before binding does not reset that boundary. Resume
captures and durably binds matching current and historical partial-import rows
strictly newer than it before replacing the database again. Native transaction
versions, rather than timestamps, order this boundary. The historical engine
must preserve its transaction high-water mark across database replacement and
physical generations. An incomplete or contradictory capture keeps the original
restore blocked; it cannot certify source deletion.

The entire read/validate/union, atomic file publication and directory sync, and
mandatory original reference-index acknowledgement run under the **same**
original journal mutex and OS writer lock. Independent journal handles cannot
lose each other's acknowledged scopes. Files publish before their index ACK;
a crash in that gap can validate and finish acknowledgement. Once
`restore_origins_required` is true, losing the entire file is a startup error.
All previous imported bindings remain in the bounded union. A pending import
without its matching original restore marker also fails startup validation.

Projection matches imported versions by complete content commitment **and**
actual imported version/timestamp. It then computes the authority's commitment
using the record's current marker at that inspection. Becoming historical does
not lose the imported origin. A later independent B record with identical
content and a different native version receives no imported authority. Empty
source-token sets still identify imported decisions, preventing causal inference
from table-batched import transactions. Versionless zero/zero rows never create
persistent version bindings; their physical erasure uses logical purge and
current membership. Their backups still carry the live planner's causal proof
for a later supported versioned import.

## Retention and validation

The sidecar belongs to the same registered finite backup generation as the data
and manifest. Cleanup removes exactly `history-origin.json` with those two files
at the original expiry. It neither renews expiry nor recursively deletes unknown
files. Imported journal state contains only opaque causal commitments and native
version correspondence; it is part of the original independent erasure journal,
not a payload backup.

Fourteen workspace controls are authored in `source_history_restore_tests.rs`:
original journal/scope/data/retention binding; tampering and canonical encoding;
actual imported-version specificity and historical marker changes; incomplete
import refusal; explicit empty imported coverage; zero-version refusal; two
independent journal handles; file-before-ACK recovery and acknowledged loss;
private file custody and byte bounds; missing causal coverage and row bounds;
actual native Mem pre-mutation rejection; owned retention cleanup; interrupted
import/retry history; and immutable version-boundary enforcement. The existing
actual named-export inspection control now expects the native sidecar.

This source checkpoint does not claim Cargo, native execution, historical-engine
physical deletion, or installed qualification. The independent native erasure
oracle and original physical-source/finite-backup-retention gate remain required.
