# Contact replacement with retained receipts

A new plain operation can publish the same immutable revision previously used
by a contact operation. Its canonical inverse removes that operation's contact
contribution while preserving independently admitted facts. The old canonical
receipts may remain available. Their retention does not make a changed shared
row a monotonic enrichment: removing B from an A+B relation requires A's original
contact lineage to be preserved explicitly in the new canonical receipt.

Replacement preparation now examines surviving support even when no commit
receipt is scheduled for deletion. The existing continuation verifier follows
the exact original support pointer, validates the independently completed contact
root and factual preservation, and captures the original controls for the same
canonical transaction. It then attaches only that verified root to the actual
new receipt. Unchanged support still takes its existing shortcut. No-op effects
are added only when an existing pointer's receipt is actually being retired.

The generic monotonic relation check remains strict. A new plain B operation
still cannot borrow B's older contact seal. Unknown support produces no contact
root; neither a current support row nor the requested replacement fence grants
new contact authority. Original epoch and private-control guards remain required.

The Runtime17 native original-A failure exposed matching current/support/A-root
fingerprints, an A+B to A inverse and an empty new receipt root set while all
three original receipts remained. Its unchanged full worker regression in
`tests/recall_preview/contact_lineage_oracles.rs` is the end-to-end qualification
gate for this repair. The additional storage controls run preparation against
actual stored legacy relations/support on both backends, assert that retained
receipts do not skip changed support, reject manufactured roots, preserve the
read-only boundary and refuse an intervening canonical epoch change. Their
synthetic request is never committed and proves no publication authority.

Only source and formatting checks were performed locally. Actual regression
execution and the full original-A qualification remain central integration work.
