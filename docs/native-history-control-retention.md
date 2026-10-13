# Native history control retention

Physical history preparation calls
`source_history_controls::retained_history_control_commitments` from the original
storage history authority. The input is the verified complete native snapshot,
the original durable erasure intent, and an unbound authority made from the same
journal identity, private key, named space, native scope, typed-table vocabulary,
and original membership. The helper checks that binding without logging secrets.
Its output contains only exact native-version HMAC commitments. B's projection
passes these inspection results to the existing `bind_history` call. They are
never stored in the history journal and must be recomputed after restore/retry.

Current source heads, authorization envelopes, source identity mappings and
operations use the original `source_publication` payload/state writers. The
helper requires the complete canonical typed payload and exact native `state`
encoding, including nested fields. It checks the physical string record identity,
space/source identity, head links to authorization and operation, and the original
intent for an erasing/erased source. Revisions and error text must be cleared as
the physical-purge writer requires. Actual `canonical_read_nonce` values from
standing/watch/contact guards are accepted only on heads and operations, with the
original canonical UUID string type. No field is silently removed.

The existing canonical publication, maintenance and source execution writers also
touch `fence_nonce` on source heads. Standing-watch and reprocess-queue authority
readers touch `standing_watch_read_nonce` and `queue_read_nonce` on canonical
epochs. Those exact fields are accepted only on their original table with a
canonical UUID string. Their complete bytes remain part of the native-version
commitment; other fields, tables and native types still refuse retention.

The external identity is deliberately unavailable after deletion. Its retained
one-way UUID key and minimal source mapping are checked as emitted; the helper
does not invent or reconstruct an external identifier. Other original current
controls have closed native field sets: tombstone references, managed-file cleanup
receipts, journal-bound membership and guard, canonical epoch, decision sequence,
outbox cursor and service grant. Membership rows and the guard count must equal
the complete durable original journal union for the named space. Unknown control
shapes receive no exemption. Existing source lifecycle exclusion lists are not
used as proof.

Current `source.erased` completion events follow the existing lifecycle's separate
retention rule. The validator reconstructs the complete original native
`SystemEventRepository` writer value from the completed original operation,
matching erased head, retained authorization and exact opaque record actions.
It requires the deterministic event UUID, native timestamps, nonnegative sequence,
empty provenance and the original `source-lifecycle` producer. A matching event
label alone cannot exempt data; changed security, extra payload, or an absent
completed operation refuses. Historical completion versions receive no exemption.

When a backup predates erasure, the original replay writer sanitizes its retained
acceptance as `Superseded`, rotates the head generation and then completes that
head. The completion event can therefore use either an original `Erased` receipt
or that completed, sanitized `Superseded` receipt. It still requires the current
`Erased` head with the same original operation and source, the original journal
intent, cleared revisions and error, and the complete event writer value. Replay
does not manufacture a later erasure operation or rewrite the old receipt.

Only four system metadata classes can receive historical retention commitments:

| Native table/key | Full writer authority |
| --- | --- |
| `publication_schema` fixed `canonical`, `source_contact`, `work_producer`, `reference_guard` keys | Exactly `id` and integer `version=1` |
| `migration_history` | Actual schema receipt name/key, native datetime, and SHA-256 of the referenced original migration SQL; the existing fake-embedding repair marker has its exact fixed name/checksum |
| `recall_durability:journal` | Exactly the original journal identity and key fingerprint |
| `native_read_fence` | Original 043 three-field schema: one of three allowed table names plus three lowercase hex bucket characters, integer version 1 and canonical UUID nonce |

These checks apply independently to every actual historical native version.
Historical source controls never inherit retention from a valid current row.
Extra members, malformed native types, unknown migration names/checksums,
substituted journal custody, missing membership or stale snapshot fingerprints
refuse preparation. The selector consumes only commitments that match the same
original inspected record bytes, version, timestamp and current/tombstone flags.

The authored workspace unit controls use the real pinned native key/value codec
and original journal files. They cover current lifecycle acceptance, historical
source rejection, all four historical system classes, injected outer/nested
payload, native numeric/datetime type changes, original read nonces, wrong
physical identity/intent, unsanitized state, missing/extra membership, restart
recomputation and changed snapshot/key custody. They are source-complete;
three further controls cover exact current completion events and reject changed
payload/authorization/producer and missing original completed operations.
execution belongs to the central validation lane.
