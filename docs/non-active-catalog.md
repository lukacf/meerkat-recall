# Non-Active Surface Catalog

This catalog separates non-active runtime surfaces into two categories.

## A. Legacy (Removed)

Definition: previously active behavior that has been replaced and removed from code/runtime.

Legacy work kinds/constants:
- `commit_assertions`
- `resolve_assertions`
- `synthesize_rels_attrs`
- `extract_match_keys`
- `resolution_sweep`
- `resolve_contradiction`

Code namespace:
- none (removed)

Policy:
- Do not use for new features.
- Prefer deletion over extension when compatibility constraints are gone.

## B. Planned (Not Yet Implemented)

Definition: intentionally scaffolded future capability that is not active in dispatcher runtime today.

Planned work kinds/constants:
- `reembed`
- `rebuild_indexes`
- `snapshot_export`

Code namespace:
- `elephant_pipeline::scheduler::work_kinds::experimental`

Policy:
- May be implemented in future phases.
- Must remain clearly marked as non-active until dispatched from pipeline runtime.
