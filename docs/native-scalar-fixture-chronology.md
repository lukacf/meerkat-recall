# Native scalar reconstruction fixture chronology

The public scalar-lag controls must start with inspectable scalar definitions
and ordinary tuples before injecting deferred rename, unique-value handoff,
source replacement, queue deletion and scalar-to-search kind transition.

The pinned `DefineIndexStatement::compute` writes a definition in its caller's
transaction, then waits for blocking index construction. The native builder's
initial scan writes populated-table tuples using `new_write_tx_ctx` and commits
those transactions itself. A tuple can therefore precede the enclosing schema
commit. The history selector deliberately refuses to use a later definition as
proof of an earlier tuple's fields.

The fixture now finishes its initial definition queries before creating any
indexed records. Before injecting lag, the versioned backend checks the actual
native inventory for each of the three original index identities: exactly one
current definition, nonempty ordinary tuple history, and no tuple version older
than that definition. Definition keys use the independent golden bytes from the
pinned `key/table/ix.rs` test; ordinary tuples use the public native decoder.

The later fixture operations and all existing assertions remain unchanged.
They still require preservation of authorized ordinary index history, complete
removal of erased delayed contributions, exact retained documents, the unique
value handoff, current indexed query results, native kind transition, deferred
schema and subsequent writes. Missing or noncontemporaneous field evidence still
fails closed in production.

R37 reported an exact-field-evidence refusal but did not retain the offending
tuple's key and version. The chronology diagnosis follows the retained native
writer and selector source; it is not a claim that a particular failing tuple
was measured. The new snapshot assertions make the fixture's version-order
precondition observable in the central run.

R38 completed that scalar-lag and kind-transition helper, including its history,
query and shutdown checks, before failing in the subsequent scalar-mapping
preflight helper. That separate setup still created its indexed document before
defining the index. It now also commits the definition first and checks the
actual native definition and all initial ordinary tuple versions before
injecting the adversarial document or replaying index bytes.

All eight mapping cases retain their original paths, initial and stored values,
expected complete tuple sets, positive retention and reopen checks, and negative
projection errors with closed transaction admission. The R38 failure did not
identify a particular tuple or mapping case; the second chronology diagnosis is
again source-supported. The mapping repair remains source-only until a later
central run supplies its actual result.
