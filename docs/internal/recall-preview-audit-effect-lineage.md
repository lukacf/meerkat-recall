# Audit effect source lineage

Maintenance has two different source scopes. Its transaction captures every
causal source needed to validate, replay, and invalidate the complete decision.
Each resulting record also declares its own source provenance. An audit snapshot
inherits the conjunction of the affected record's trusted before and after
images through `PlanningTruthMaintenanceStorage::create_audit_log`. If an
aggregate drops source A from its new membership, its audit still contains A's
old data and therefore retains A alongside the surviving sources.

`maintenance_supports` must use that per-record provenance for audit snapshots,
as it already does for other canonical effects. Attaching every transaction
source to every audit invents additional record support. The transaction's source
union, source fences, decision receipt, inverse effects, and replay guards remain
intact when per-record support is corrected. The shared `inherit_maintenance_audit_authority`
helper reads only images of the affected record from the prepared effects or
observed dependencies. It rejects missing or foreign authority. Snapshot
identities and sources must fit those trusted images; policy, classification,
handling, and subject constraints are combined without widening. The canonical
boundary independently checks that the submitted audit has that envelope.

The source108 provider100 original exposed this distinction. One completed
household maintenance decision contained 48 audit records, each declaring one
source also present in its before/after snapshots. The storage support ledger
instead added 576 rows: each audit was linked to all 12 transaction sources.
The private capture correctly charged the resulting physical rows. Completed
captures used 3,799 of the unchanged 4,096-row allocation; the final business
capture needed 941 additional rows and failed with `capture_row_limit`.

The repair changes the source attribution that creates these records. It does
not omit audits, alter payloads, raise capture limits, change logical-row limits,
or reinterpret a failed capture as complete. The original run remains failed
and unqualified despite its 28 completed source publications.

Regression source covers the observed 48-audit/12-source batch, genuine
multi-source conjunction, before-only sources, source order, exact published
revisions, missing and forged authority, creation/deletion, source-free targets,
and control-only effects. Both backend regression sources retain full decision
invalidation and reject replay after withdrawal of a causal input.
Transaction-wide causal sources remain present for inverse and replay authority.
Source-only preparation does not establish current build, test, capture-fit, or
provider qualification.
