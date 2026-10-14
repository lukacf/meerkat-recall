# Producer bookkeeping and original planning epochs

Creating a validator/commit child writes a new private queue-origin row after its parent has prepared a semantic decision. That registration and an exact parent-origin read nonce must not invalidate the parent's original epoch. Re-capturing the epoch would discard the original read-set protection.

For `work_producer_origin` only, the kernel and migration 041 exclude insertion and updates changing only `read_nonce` from canonical epochs. Missing origin never qualifies as producer authority; existing parent/job origins remain mandatory and their full semantic rows are guarded by the producer owner. Every deletion and every other field change, including payload, identity, reference projections and space moves, still advances the affected epochs. All erasure guards, private reference revisions and native exact-read contention remain active.

Saved reprocess selection/intent artifacts retain their separate narrow epoch rule. Source-effect support and generic extension fencing are unchanged.

Test source covers real queue creation on both storage backends, nonce updates, semantic corruption, and kernel replace/upsert/delete/move parity. These are source-only oracles; no local Cargo or Rust tests have been run.
