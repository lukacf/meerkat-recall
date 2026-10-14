# Native write preparation

The pinned native engine resolves the current field definitions before creating
the initial/current document diff. When there are no definitions, no field
expression can consume that diff, so processing returns after the original
record-id check and field lookup. The lookup remains per operation and retains
the original table cache generation, transaction, version and errors. Defined
fields still receive the complete original diff and field context.

This does not skip the separate NONE cleanup, table permissions, record and
index writes, events, changefeeds or transaction rollback. In particular, every
original ancestry nonce update still executes its original event path. The
producer graph limits and native 240-second acceptance test are unchanged.

Byte-key trace fields still eagerly produce the same escaped String. The
formatter uses the standard slice `escape_ascii` display implementation, which
writes printable ASCII runs together and uses the original `escape_default`
representation for escaped bytes. It preserves trace field types and log
fallback behavior; no tracing configuration or logging filter changes.

Storage controls exercise empty-definition writes, nested NONE cleanup, ordered
events and rollback, late field definitions, original changed input, defaults,
assertions and readonly fields. A source inclusion of the actual byte-key
formatter compares both byte implementations and range/list composition with
the original iterator for every possible byte and mixed strings. These are
correctness controls; the original full native ancestry test remains the
performance oracle. No performance improvement is claimed from source review.
