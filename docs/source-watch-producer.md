# Original watch execution authority

Queued watch refreshes capture `PreparedSourceWatchProducer` before fetching.
It binds the actual Watch, WorkItem, WorkRun, service actor, and original
`AttemptFence`. Source acceptance checks the original live lease and full Watch
and Run snapshots inside the same transaction as the head, immutable revision,
operation receipt, Doc observations and optional contact admission. WorkItem
intent checks permit lease renewal without adopting a different claim.

Native checks touch the observed Watch and queue rows to conflict with concurrent
mutation. The Watch read fence follows any contact epoch check; it introduces no
schema field. Observation success, unchanged, failure and unsupported results
must use I's guarded `record_check` adapter with this same producer. A stale
attempt must propagate lost lease rather than write an unfenced failed check.
Source head CAS alone does not establish watch execution ownership.

Accepted operations store `SourceProducerOrigin::Watch`, including the original
watch fingerprint, work/run identity, attempt and B's immutable work-intent
fingerprint. `verify_watch_origin` verifies historical hot/archive execution
identity without requiring the old lease still be live. It grants no publication
or unlimited-budget authority: M continues the bounded private work-producer
chain, retaining original finite restrictions.

Existing matching-intent receipt lookup remains read-only. Its old pending
status does not authorize a stale watch to enqueue children. I/B must preserve
original attempt authority for queue effects or treat that replay as receipt-only;
durable recovery retains its independent obligation to dispatch accepted work.

Six storage tests exercise actual claims and acceptance on both backends:
original Watch origin, historical intent verification, cancelled original claims,
and changed Watch authority. B owns the actual dispatcher/fetch-barrier matrix
and I owns guarded observation and enqueue consumers. Source authored only;
no Cargo validation ran in the integrity worktree. This group requires A/I/B/M
consumer joins and the I-owned `watch_to_value` crate visibility change before
central validation.

The additive `accept_with_watch_producer_outcome` returns ephemeral
`WatchSourceAcceptance::{Accepted, ReceiptOnly}` from the actual transaction.
This distinction is made in all three existing-operation paths: the original
lookup, the embedded transaction's second lookup, and the native competing-write
fallback. An earlier `NewIntent` lookup is insufficient evidence for enqueue.
The original receipt-returning method remains available. Two additional backend
oracles race matching calls and require exactly one actual insert classification
and one receipt-only result with identical operation identity.

Queue intent also binds the requested watch configuration before a claim exists.
The I scheduler stores A's `watch_intent_fingerprint` in the immutable work
payload and creates a distinct queue identity when that intent changes. A capture
checks the fingerprint and exact work/watch envelope and provenance. A queued
watch-ID-only item cannot acquire a new URI, interval, source or policy by reading
the latest Watch. Observation counters and next-check timestamps are excluded.
Missing historical intent is unknown; it is not adopted as current authority.
Two backend tests verify changed-URI refusal and observation-only hash stability.
