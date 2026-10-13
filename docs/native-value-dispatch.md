# Native value dispatch allocation

The native producer ancestry path retains its complete original observation and
transaction guards. Each queue row still evaluates both hot and archive SELECTs,
compares the original native fields, and performs the ordinary nonce UPDATE with
its full output, field processing, events, reference guards and conflict writes.
The private origin guard retains its separate SELECT and UPDATE. No observation
is reused as authority for a later commit.

The pinned evaluator now schedules its four remaining inline recursive branches
(cast, function, model and references) on its existing reblessive stack. The
other recursive value branches already use this mechanism. This removes those
large nested future states from the dispatcher used for every scalar field.
Each selected branch receives the original context, document and Options after
exactly one depth decrement. The existing outer RETURN boundary is unchanged.
There is no new builtin, native query, cache, schema or publication authority.

The private `dispatch_tests` keep the former dispatcher as a frozen oracle for
future size, depth limits, first errors and RETURN handling. The workspace
`native_value_dispatch_tests` exercise durable same-row Future writes, mandatory
cold evaluation, nonce-event/output order and transactional rollback. Existing
native-value, permission, producer and concurrency controls remain required.

These tests are source controls until executed. A smaller dispatcher does not
establish that the original 256-work/257-source ancestry test passes its unchanged
deadline. That performance result requires the original complete test run.
