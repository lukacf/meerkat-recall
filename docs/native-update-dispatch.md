# Native UPDATE dispatch

The native ancestry boundary test still exceeded its original 240-second
deadline on source `446b79e0679d5ccbdbf84caa7df25b3ba9cf39e7`: construction of
256 actual Work nodes reached 237,268 ms before the first dispatch walk. The
other 704 selected tests passed. This result does not qualify the native ancestry
boundary or demonstrate a speed improvement from the earlier point-read change.

Each successor admission retains every original ancestor comparison and changing
conflict nonce. Across the 255 admissions, the chain requires 97,920 nonce
updates, in addition to their original events and other transaction work. The
earlier bounded diagnostic attributed most sampled construction work to this
whole guard phase; it did not establish an exclusive cost for UPDATE dispatch.

`UpdateStatement::compute` now schedules its existing `Iterator::prepare` and
`Iterator::output` futures separately on the existing `Stk`. This removes their
direct embedding from the caller future. Both method calls, their arguments,
their order, and their returned results are unchanged. The original target-error
mapping stays after preparation, and the existing timeout and ONLY checks stay
after output. The general iterator, its depth and cancellation checks, document
mutation pipeline, savepoints, permissions, field processing, index writes,
events, and returned values all remain in place.

This change introduces no point-write implementation or eligibility shortcut.
Every original hot/cold row comparison, authority check, nonce write and event
still executes. The 256 Work and 257 Source boundaries and the 240-second test
deadline are unchanged. Actual speed and runtime qualification require the next
centrally coordinated run.

The existing native controls exercise the changed calls:

- `native_document_dispatch_keeps_crud_retry_events_and_relation_results`
  checks UPDATE results and the ordered mutation event trace.
- `native_document_dispatch_keeps_unique_retry_identity_and_transaction_rollback`
  checks existing record identity, unique retry behavior, and transactional
  rollback after mutation.
- `native_empty_field_set_preserves_none_cleanup_and_event_rollback`,
  `native_empty_field_set_reloads_later_definitions_and_original_input`, and
  `native_defined_field_assertions_and_readonly_errors_still_rollback` cover
  real nonce updates, nested NONE cleanup, current field definitions, original
  values, assertion errors, readonly errors, and rollback.
- `native_identity_data_keeps_original_update_event_and_rollback` checks that
  the original UPDATE event can refuse a mutation and preserve the earlier row.
- `native_point_select_keeps_target_projection_effects_and_transaction_rollback`
  includes UPDATEs from an effectful target function and the original event
  trace within a transaction.

All original selected controls are retained. No additional test or build was
run while preparing this source change.
