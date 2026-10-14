# Canonical outbox sequence allocation

A canonical transaction captures its named space's highest existing system-event
sequence before applying any effect. Replacement can physically retire earlier
events, including the current tail. Those deletions cannot lower the captured
floor: a consumer may already have observed their sequence numbers.

An explicit retained or restored `SystemEvent` effect can carry a higher sequence.
The same allocation also accounts for every such guarded after-image and the
durable floor retained by earlier event transactions. New outbox events receive
consecutive integers above all three floors, in the plan's original outbox order.
The whole batch must fit the signed 64-bit range. Exhaustion aborts all effects,
event writes, epoch changes, floor updates and the receipt.

Both exposed backends use this rule inside the existing canonical transaction.
Native storage retains the explicit `SELECT seq` row projection and scalar field
access, plus the unique `(space_id, seq)` index. The shared `event_sequence`
storage helper records the reserved batch end in one private producer record in
the existing `outbox_checkpoint` table. That record is not a consumer
acknowledgement: it records emitted history, including history that retention or
erasure has removed. Public checkpoint load and advance reject its reserved
identity. It contains no event payload or source identity. Reservation and event
writes use the same transaction; their shared floor write also fences concurrent
retirement and allocation. Exact receipt replay returns the original result
without allocating again.

A canonical transaction preserves the original floor even when its effects
delete every event and it emits no new outbox rows. A later transaction therefore
cannot reuse those numbers. Knowledge-view publication reserves the entire batch
after checking every original sibling guard, before writing any view, revision,
dependency index or event. The standalone outbox writer reserves and creates in
one storage transaction; its native conflict classifier, 20-attempt limit and
backoff remain unchanged, and embedded writes retain one attempt. A successful
single-event append is not a canonical receipt or a new replay authority.

The focused transaction controls publish three real events, retire the highest
two while publishing two replacements, and require sequences 4 and 5. They check
physical membership, unchanged retained payloads, fault rollback, exact replay,
another named space with a higher tail, an explicit higher event effect and
whole-batch exhaustion. A stored maximum cannot be deleted to make room for a
new outbox event; a batch without new outbox events needs no further sequence.
Two further backend controls delete the complete event history in a successful
canonical transaction with no new events, then require the next publication to
exceed the original tail. A higher explicit after-image followed by an injected
late failure must not leak its reservation into that subsequent publication.
These controls inspect actual events and receipts rather than the helper's floor.
Independent public API scenarios cover retention, standalone writes, whole
knowledge-view batches, concurrency and native event-write faults; lifecycle
controls cover erasure and durable reopen. The original source-survivor
regressions remain necessary integration coverage. Authored controls are not
runtime validation until they execute.

The native sequence fixture installs the real private reference-fence migration
(`039_private_reference_fence.surql`), including its table events, in addition to
the shared canonical fixture's publication schema. Successful retirement must
pass the production guard. The rollback controls require the exact injected
fault message, so a missing schema function cannot count as a rollback result.

The floor cannot reconstruct sequence numbers erased before this repair was
installed. Snapshot rewind of external consumer cursors is a separate contract;
restoring the retained bookkeeping record alone does not resolve that rewind.
