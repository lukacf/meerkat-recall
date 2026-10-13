# Accepted source revisions at an existing ingest mode

Runtime17 at `4979bb883adde3d8fa884d6e820a6a1a1a76dfc4` reached the real restore dispatcher and failed `test_restore_deleted_doc` with `Invalid state transition from graph to graph`. Restore had correctly accepted a new immutable revision and document identity while retaining the historical Graph mode. The accepted-work dispatcher correctly bypasses the legacy already-at-target skip and must render and finalize this new revision.

`execute_upgrade_ingest_mode_for_work_attempt` now selects explicit `AcceptedRevision` admission. It permits Graph-to-Graph and Retrieval-to-Retrieval rebuilds, then performs the existing daily budget, item-cap, and concurrency checks. It still rejects downward transitions and same-mode Skip or Metadata requests. The public legacy helper selects `StrictUpgrade` admission and still requires a higher target mode. Only strict legacy admission may enqueue legacy budget-deferred upgrade work; accepted work retains its original attempt and publication through its caller.

This change does not edit the dispatcher, source lifecycle, original source payload, accepted authority, rendering, finalization, or the restore E2E assertion. It neither marks a source complete nor fabricates a work claim. The existing actual `_publication` branch chooses the accepted helper; no latest-head reconstruction is introduced.

Two authored backend contract tests exercise both accepted same-mode positives, exact legacy transition errors, denied daily budgets under `DegradeModeWithQueue`, invalid accepted transitions, retained source bytes, no unbound queued work, and no additional cost reservation or reconciliation. Existing strict-upgrade tests and the original restore E2E remain the integration oracles.

Validation: source completed before any validation request. Direct Rust formatting and Git whitespace checks only; no Cargo/build/test/clippy, services, providers, or model probes executed by intake. Runtime validation remains with the central integration runner on the next coherent source checkpoint.
