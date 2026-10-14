# Durable event sequence floor

Event sequences must remain above previously emitted values when source erasure,
replacement, retention, or a retained event update removes the physical tail.
Consumers and API clients keep their original cursors and request `seq > cursor`.
A surviving-row maximum alone can hide later notifications after deletion.

`elephant_storage::event_sequence` uses the existing `outbox_checkpoint` table
for one private producer row per named space. Its reserved consumer identity is
`__recall_event_floor_v1`; public checkpoint `load` and `advance` reject that
identity before storage access. Ordinary acknowledgment rows and their compare
and swap behavior remain unchanged. The producer floor is never inferred from
consumer acknowledgments or the separate source-decision sequence.

The reserved row contains exactly `id`, `space_id`, `consumer`, nonnegative
signed-64-bit `seq`, and native `updated_at`. It reuses the checkpoint's existing
length-framed UUID key. Readers reject malformed or differently scoped rows,
alternate keys, extra payload fields, and invalid integer sequences. The row has
no source content, source IDs, event IDs, credentials, or provider responses.

`reserve_tx` and `append_reserve_sql` run inside the caller's original
transaction. They take the maximum of the retained floor, live same-space event
sequences, and guarded explicit event after-images, validate the entire batch's
signed integer capacity, persist its end, and return its base. Event creation
belongs to that same transaction; failed creation rolls back the floor as well.
A zero-count reservation preserves the original tail before destructive writes.
The shared producer row serializes native allocation with history retirement.

Source retraction preserves the floor before its plan mutates events. Physical
erasure preserves it in the purge transaction, before any events disappear.
That row survives the later completion transaction and recovery after purge or
persistence faults, so `source.erased` allocates above the original cursor.
The existing erasure journal, retained controls, exact source/support guards,
physical inventory and completed-erasure proof remain unchanged.

Retention delegates to `retire_before`. Embedded retention preserves affected
space floors and removes old events in one mutation. Native retention discovers
candidate spaces, then preserves the current floor and deletes matching old
events in one transaction per space. A concurrently introduced space waits for
the next pass; it is never deleted without its floor. Sequence-lowering updates
also preserve the original floor in the transaction that updates the event.
Ordinary soft deletion retains the physical sequence.

Canonical commits, knowledge-view publication batches, standalone outbox writes
and lifecycle notifications all use the same allocation helper. The public
`append_system_event` primitive allocates and creates atomically and returns the
actual assigned sequence. It adds no configured-space or source-publication
permission, performs no retries, and treats an existing ID as a conflict rather
than a replay certificate. Caller payloads and the existing retry policy remain
caller-owned. Exact canonical receipts and lifecycle idempotency still belong
to their original owners.

The existing checkpoint table is retained through source erasure and current
snapshots; managed restart retains it with the original durable store. This
cannot reconstruct already-deleted pre-repair history or guarantee an external
cursor across an arbitrary historical snapshot rewind. No generic migration or
new source authority is introduced.

Focused source controls cover reserved-identity refusal, malformed producer
rows and rollback after real event creation. Independent lifecycle controls
exercise real purge faults, durable reopen, current snapshot recovery, retention
and consumerless cursors. Independent public writer and knowledge-view controls
cover actual retention and sequence lowering, named-space separation, concurrent
writers, signed overflow and publication rollback. Source review and formatting
are separate from central runtime validation; no local Cargo run is claimed.
