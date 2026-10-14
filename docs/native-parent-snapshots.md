# Native subquery parent snapshots

Subqueries capture the current document as `$parent`. An event already owns an
immutable reference to its before and after documents. Capturing that reference
can share its allocation: later cursor mutation uses the existing copy-on-write
path and cannot change the captured value. A cursor that still owns a mutable
value makes the same independent deep copy as before. Snapshot capture never
changes the cursor's ownership state or stores an evaluated result.

All subquery evaluation, context construction, permissions, depth checks, reads,
writes, events and outputs remain unchanged. Stored futures remain deferred in
the snapshot and execute whenever the original evaluator reaches them; sharing
the immutable syntax does not memoize their result.

The native storage regressions exercise a normal SELECT parent, a mutable parent
during sequential UPDATE assignments, and an event parent. They check a nested
write observed through the database while the event retains its original
before/after images, repeated stored-future effects, and transaction
rollback after those effects. A private cursor unit test additionally checks that
snapshots survive mutation from both ownership states. The vendor unit test is
separate from the workspace storage test selection and must not be counted as an
executed workspace test.

The rollback controls inspect both native statement errors: the earlier completed
write becomes `QueryNotExecuted`, and the later statement retains its exact
`Thrown` sentinel. The SDK's first-error `check()` returns the former, so it cannot
establish that execution reached the deliberate failure. The original effect
counts and stored-state rollback assertions remain required.

These are correctness controls. Performance remains subject to the unchanged
native 256/257 ancestry boundary test and its original 240-second deadline.
