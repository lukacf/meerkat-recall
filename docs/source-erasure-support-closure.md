# Source erasure and support closure

A source support row certifies a particular canonical record. If physical source
erasure deletes that record, every support row naming it must also be deleted,
including support from a surviving source and support marked independent.
Independence can preserve a factual baseline; it cannot preserve a ledger entry
for an audit or other payload row that no longer exists.

Public independent status updates create audits carrying the participating source
history. An A+B audit can therefore disappear when B is erased while the relation
keeps the independently requested status and A remains its verified support.
Erasure collects support deletions from both decision inverses and actual physical
dependency-closure deletions. Retraction adds no deletion for retained payloads.
If an earlier purge committed the record deletion but left another source's
support behind, replay recognizes the deleted record's exact typed identity in
the original durable journal. It removes the orphan without inferring ownership
from a stale receipt pointer or deleting support for a surviving fact. The orphan
branch requires the record to be absent from the complete original inventory or
actually deleted by this plan; contradictory surviving rows fail closed.
Deleted-identity membership is separate from historical-version commitments, so
a historical source-bearing version alone cannot remove a current fact's support.
All deleted support identities enter the existing durable erasure membership in
the same reserved purge. Native current-version classification, original-journal
checks, physical history deletion, and the readiness barrier are unchanged.

The repository regressions verify both backends, independent support flags,
retained source heads, raw absence, and the different retraction obligation. The
actual contact/public-mutation composition regression captures both physical audit
support keys before erasure and requires both absent afterward, while retaining
its existing exact relation/status, original-support, and maintenance assertions.
An interrupted-purge backend pair reproduces the old support-selection omission
through the real reserved transaction and journal writer, then requires real
recovery to remove the orphan, preserve the other source, and keep repeated replay
ready. It does not fabricate memberships or bypass native classification.
These source changes have not yet been executed; validation is centrally owned.
