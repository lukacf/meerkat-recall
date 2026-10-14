# Reprocess controls while source publication is pending

A budget refusal must be able to pause an existing reprocess job whose selected source has only a desired revision. Requiring a published revision prevents recording that refusal; discarding source provenance would discard authority.

`CanonicalRepository::capture_reprocess_control_support(space, original_job)` captures exact current source input/head snapshots, including pending desired input, plus retained causal job authority. `CanonicalMaintenanceOrigin::ControlOnly` permits one existing job update restricted to status, progress, started/completed time, error and bookkeeping update time, with matching new audit rows and events affecting only that job or those audits. Selection, budget, identity, policy, provenance and other immutable inputs remain exact. Source head and source payload, original job, queue attempt and planning epoch are guarded in the normal atomic canonical commit.

The `PendingControl` fence is rejected for ordinary derived or independent canonical mutations. Control-only commits cannot create or delete jobs, mutate graph or extensions, mint/update source-effect support, create source-decision authority, or advance source publication. The full canonical effect/outbox/receipt transaction and replay checks remain active. M owns the dedicated job-control caller; its original preparation and configuration checks remain required.

Both-backend test source starts from actual accepted but unpublished input, refuses graph/budget/provenance/mode escapes without a receipt, accepts and replays the job pause plus exact audit/outbox, and verifies the source remains unpublished and no source support is created. These tests have not been executed locally; central integration owns validation.
