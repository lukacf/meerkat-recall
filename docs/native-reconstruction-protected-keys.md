# Protected keys during native index reconstruction

The memory and Rocks erasure owners compare protected native key/value streams
before and after rebuilding selected indexes. The authorized ordinary index
prefix remains unchanged. Full-text reconstruction also writes the native `Bs`
state key, whose layout is table-local (`!bs{index}`), outside that prefix.

The shared reconstruction helper derives an additional set of exact keys from
the retained current `Search` index targets. Memory and Rocks use equality with
that set when hashing protected data. They do not exempt a table, a general
`!bs` prefix, another index, or a malformed key with a matching initial string.
Unrelated documents, definitions, counters and indexes remain in the protected
comparison. The selector still removes the old aggregate state before rebuilding
from retained documents; this change does not authorize deleting live records.

The memory fingerprint control writes the exact state key into a real in-memory
native transaction and checks that its protected digest stays unchanged. Changes
to another namespace, database, table or index, a suffixed state key, a schema key
and an independent record must change the digest. Existing public memory and
Rocks reconstruction controls exercise the complete active path.

The full-text inventory fixture first requires the original live-source refusal,
then performs the ordinary SQL deletion before inspecting physical selection.
It requires the real full-text state to be dropped and rebuilt while preserving
the independent document. A missing purged record is not an inspected row that
the selector can authorize.

The duplicate-unique reconstruction fixture creates its index definition before
the first ordinary tuple. The native populated-index builder uses separate
transactions; an earlier tuple cannot borrow a later schema version as historical
evidence. The fixture keeps that refusal intact and supplies the ordered schema
and tuple needed to reach the intended native uniqueness conflict.

These are source repairs and authored controls. Compilation, private controls,
public reconstruction tests and installed erasure acceptance require separate
centrally scheduled results on the coherent joined source.
