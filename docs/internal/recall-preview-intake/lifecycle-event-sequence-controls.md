# Lifecycle event sequence controls

The source lifecycle sequence regressions retain an external cursor captured
from actual original events before erasure or retention. The expected next
sequence comes from that original history, never from the new sequence helper.
The same cases run against embedded storage and native SurrealDB.

The real `erase_live_with_fault` entry stops after physical purge and after
persistence. At each boundary, both source-owned tail events and the Source are
physically absent, the independent event remains, and the erasure completion has
not yet been emitted. The test exports and replaces the current physical snapshot
under the actual recovery barrier, then replays the original independent journal.
The single completion must follow the original cursor, preserve the other named
space exactly, and contain no erased event payload. Replaying recovery again
must preserve its exact identity and sequence.

Both a previously acknowledged consumer and a consumerless external cursor are
covered. Producer bookkeeping must leave the real consumer checkpoint unchanged.
These storage tests do not claim to execute a consumer delivery callback. Existing
pipeline delivery controls separately exercise success-aware acknowledgment.

An additional embedded case drops every managed store handle at both fault
boundaries and reopens the actual original durable store and journal. It performs
no shutdown export or direct floor write. This is real durable reopen coverage;
the native snapshot replacement controls do not claim native process-crash or
external server restart qualification.

A late in-transaction purge fault must preserve the exact original checkpoint
population and every original event, then recover through the actual lifecycle
entry. Real `event_sequence::retire_before`, which the retention worker uses,
also deletes the entire old event population in one space. After current-snapshot
replacement, a real source retraction must still be visible above the original
consumerless cursor. The other named space and absent consumer checkpoint remain
unchanged.

All nine new test definitions are under
`source_lifecycle::tests::sequence_tests`. Original lifecycle regressions are
unchanged. The module is registered by the event-floor implementation owner.
The tests require that implementation and its writer integration. Source was
formatted and statically inspected only; central compilation and runtime
validation remain required. Historical snapshot rewind against arbitrary newer
external cursors, recovery of unknown event history deleted before this repair,
and installed fault qualification are separate obligations.
