# Native reference guard evaluation

The native reference guard reads private journal custody, checks reservations
and membership completeness, validates the complete typed reference input, and
updates the shared erasure revision in the writing transaction. Queue ancestry
nonce writes use these same events. Their transaction conflicts and rollback
behavior remain part of the guard contract.

The guard and membership tables are schemaless. A selected row can contain the
result of a stored SurrealDB future. Reusing that result does not prove that a
second SELECT would have the same effects. Similarly, a guard CREATE can invoke
an event that changes the actual row before its next read. Performance changes
must preserve these evaluation boundaries without assuming that all stored rows
were written by the ordinary Rust metadata adapter.

`reference_guard_evaluation_tests.rs` exercises the current function and a frozen
original function independently against native in-memory databases. It checks
repeated stored-future effects and their rollback, post-CREATE event observations
and refusal ordering, and full encoded-reference validation with empty membership.
The metadata-only path retains its separate reference-check behavior and shared
revision write. The frozen SQL is a test oracle and is never installed by runtime
initialization or restore.

These controls establish correctness boundaries when executed. They do not
measure ancestry performance or replace its original Work/Source boundary test.
