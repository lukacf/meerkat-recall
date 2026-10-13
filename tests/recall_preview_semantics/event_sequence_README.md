# Original event sequence qualification controls

These eight source controls call the actual `OutboxWriter`,
`SystemEventRepository`, `RetentionWorker` and `KnowledgeViewRepository`.
They run through the existing scenarios binary and use its existing dependencies.
Native setup applies the real schema; embedded setup uses the real storage engine.
There is no provider call, transport spy, private floor helper or replacement writer.

The expected floor is the highest sequence of the original events returned by the
actual writer, captured before mutation. Real retention physically removes the
first two original events. The actual repository then lowers the remaining tail
to the vacated first sequence, and real retention removes it as well. A subsequent
writer must emit exactly one above the original tail. Seven unretired events in a
second named space must neither raise that floor nor change. Consumer checkpoints
retain their original acknowledged value; a different space's absent checkpoint
stays absent as observed through its zero cursor.

Both backend controls exercise concurrent actual outbox writes and demand two
distinct IDs at the next two sequences. Separate controls publish two real
knowledge views with an actual source-less entity dependency each, then check the
view, revision, dependency index and exact event sequences. The original epoch
must reject another prepared view after successful publication without changing
its rows, the event inventory or the new epoch.

The overflow controls create an explicit original `i64::MAX - 1` event, physically
retire it, and require a two-view batch to refuse atomically. An actual outbox write
must still consume the one remaining sequence, proving the failed batch did not
reserve a prefix. Lowering and retiring that last event cannot restore capacity:
both the next outbox write and a single view publication must refuse, preserving
all original events, epoch and unpublished rows. No live maximum remains to hide
a missing durable floor.

Two additional native controls install a test-only database event which throws
on the actual event CREATE. The original refusal text must be observed. Removing
the fault and retrying the original writer or original prepared view batch must
use the next original sequence without a gap. The knowledge-view case also
requires rollback of its view, revision, indexes and epoch. This tests the actual
same-transaction create boundary, beyond an early capacity check.

These are authored source controls, not executed outcomes. They depend on M's
durable sequence preservation for real retention and repository lowering, and F's
atomic outbox and knowledge-view allocator integration. They do not qualify
process restart, source erasure, full consumer delivery, backup expiry or semantic
provider quality. E owns the distinct erasure/recovery/reopen controls. Root alone
schedules the coherent central runtime gate; no local Cargo or backend execution
has been performed for this source checkpoint.

Selectors under `recall_preview_providers::event_sequence` (the actual binary
prefix is supplied by central nextest inventory):

- `embedded_outbox_retirement_preserves_original_floor`
- `native_outbox_retirement_preserves_original_floor`
- `embedded_knowledge_view_retirement_preserves_original_floor`
- `native_knowledge_view_retirement_preserves_original_floor`
- `embedded_retired_sequence_overflow_refuses_entire_publication`
- `native_retired_sequence_overflow_refuses_entire_publication`
- `native_outbox_create_failure_rolls_back_floor_reservation`
- `native_view_event_failure_rolls_back_floor_and_publication`
