# Reprocess recovery observation and contention

Parent: `15745df0399382a9579fe101b6ed7cda404951f6` (source31). This is the
queue portion of the next source checkpoint; independent hybrid/wiki repairs
must be integrated before Root selects another native/artifact validation.

The closed source31 primary run recorded three commit conflicts for business
reprocess work `733b7516-7e82-5c85-9e4a-637b9846a295`, followed by terminal
failure. The original job remained Pending because its progress/status update
was part of the transaction that failed. This establishes execution and failed
commits, rather than a missing dispatch or managed worker. It does not identify
the exclusive concurrent writer behind each failure.

The source nevertheless establishes an unnecessary contention edge. Every
fallback recovery pass calls `ensure_work` for Pending/Running jobs. Previously,
the Surreal transaction always wrote both `work_producer_origin.read_nonce` and
`canonical_epoch.queue_read_nonce`, including for an already valid hot or archived
handoff. Canonical reprocess publication also writes the epoch row; observing
already owned work could therefore invalidate an executor's transaction.

The existing transaction now always checks the original producer payload, exact
observed job, epoch presence, and full hot/archive work envelope. It writes the
two recovery fences only in the branch that creates missing work. Existing work
retains its original state, attempt, lease, payload, and archive wrapper. The
`_attempt_fence` exception remains limited to runtime ownership evidence; it does
not relax job identity, security, subjects, provenance, kind, or idempotency.

Creating the initial job and queue handoff remains atomic. Repairing a retained
job-to-work crash gap still checks and touches both evidence fences in the same
transaction before enqueue. Unknown private producers, changed jobs, altered
work evidence, and invalid archive wrappers remain errors. Other private-origin
mutation guards and the embedded storage path are unchanged. Observation never
grants a claim or publication authority, and no retry limit is increased.

Three new native database regression tests are authored in
`crates/storage/src/reprocess_queue_observation_tests.rs`:

- Repeated recovery leaves complete job, hot/archive work, producer, attempt,
  canonical epoch, and reference-guard rows unchanged for Pending/Running jobs
  and runnable, claimed, failed, and archived work.
- A real committed intent without a handoff is repaired and still writes both
  producer and epoch nonces; subsequent recovery is observational.
- Changed job snapshots, hot security/payload, malformed/missing/moved private
  origins, and changed/deleted/moved archive wrappers or archived security are
  refused without any further row mutation.

The two existing embedded/Surreal queue contract tests retain their atomic
creation/rollback, all-space orphan recovery, stale security, unknown producer,
terminal retention, and archive checks. All new controls invoke the active
repository path and inspect actual database records rather than SQL text.

No Cargo/build/test/native/provider execution was performed for this source
checkpoint by grounding. Root selects validation after the coherent source batch
and owner reviews. The original source31 primary failure, two hybrid misses,
and incomplete wiki cases remain immutable evidence; this queue change alone
is not a semantic qualification claim.
