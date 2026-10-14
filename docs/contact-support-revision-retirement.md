# Contact support revision retirement

The existing embedded and native survivor scenarios now check the entire
persisted support population for the original relationship immediately after
an actual, independently admitted replacement publication. A positive current
contact support proof alone cannot exclude an extra support row from an older
revision.

The check binds the observed published head to the already captured successor
fence, requires a different revision from the original accepted publication,
and requires exactly one support row for that source and relationship. Its
revision, semantic fingerprint and writer must match the original successor,
current relationship and durable completion receipt. `support_for_record`
reads all stored support payloads for the record; it does not filter by the
current published revision. The observed head is used only for assertions and
never replaces the original reprocess attempt or publication authority.

These assertions run after the existing accepted-contact positive and before
the existing original-attempt completion. The source-erasure, coordinator,
maintenance, certificate and policy assertions remain unchanged. Repository
awaits remain boxed inside the existing phase split; no new slow fixture,
provider request or manufactured publication proof is introduced.

Runtime19 at `bf9d4366c41cb4dbd02fc981eda9c7f54ac0dcc0` exposed the relevant
negative state in both native survivor scenarios: an old `shared-contact-b`
support row remained beside the new `reprocess-*` row, with matching current
fingerprints and writer IDs. The actual head published the new revision. The
strict maintenance guard rejected the stale row. The new earlier assertion is
a regression check for retirement, while the unchanged later assertions still
check the complete survivor behavior.

This source checkpoint has not been compiled or executed. Validation belongs
to the centrally scheduled coherent storage repair, including both original
native survivor positives and their embedded counterparts.
