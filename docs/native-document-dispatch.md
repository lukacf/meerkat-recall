# Native document dispatch

The native engine dispatches each document's selected SELECT, CREATE, UPSERT,
UPDATE, RELATE, DELETE or INSERT operation through its existing reblessive async
stack. This keeps the largest mutation future out of the outer dispatcher used
for every document, including point reads in producer ancestry transactions.

The selected method receives the same document, context, options and statement.
Record generation, cancellation checks, savepoint creation, retry-with-ID reads,
rollback, savepoint release and result handling retain their original order.
No extra option depth decrement or independently spawned task is introduced. Each operation's
permission checks, field processing, stored futures, indexes, events and output
remain in the existing document methods. The unused channel dispatcher is
unchanged.

Producer ancestry still observes every original ancestor and evaluates both hot
and archive reads before each original row guard. Every canonical and private
origin nonce write and its event processing remains present. No parent cache,
summary authority, new builtin or deferred guard replaces those operations.

The workspace controls exercise all seven operations, existing-ID and unique-key
retries, stable record identity, ordered events, relation results, deletion and
whole-transaction rollback. Existing native dispatch controls additionally cover
stored-future writes, unconditional archive evaluation, event/output order and
first-error rollback. Existing permission and ancestry controls remain required.

Source review and formatting do not establish runtime correctness or speed. The
complete original 256-work/257-source test with its unchanged 240-second deadline
is required to determine whether this allocation change resolves the boundary
failure.
