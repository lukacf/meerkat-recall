# Existing lifecycle and the RTBF question

Static clarification, 6 October 2026, at current upstream main `f59e6f5c99ed095a015656626f4d32c106c13075`. The user challenged the proposed "general person-level purge and advanced correction/restoration" deferral, recalling early GDPR/right-to-be-forgotten work. No product builds, tests, linters, scripts or runtime probes were executed for this follow-up.

## Planning correction

**Withdraw the blanket correction/restoration deferral.** Preserve and qualify the substantial existing lifecycle implementation. Reuse its guarded storage operations, public endpoints, temporal machinery, retention and recovery tests. The remaining task is to make their source-head, authority, publication, propagation and erasure interactions coherent for supported workflows.

Full source/person erasure is a separate coverage question. A dedicated person-targeted RTBF workflow was not found in the inspected source or retained history. This does not prove it never existed, and does not authorize a feature cut. If another branch or source contains it, reconcile and reuse it before designing a replacement.

## Implemented functionality to credit

| Capability | Current source evidence |
| --- | --- |
| Soft deletion and restoration | Registered MCP operations for entities, relationships, documents and sources; REST operations also include assertions. [MCP registration](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/mcp/src/handlers/mod.rs#L314-L405), [assertion contract](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/docs/api/assertions.mdx#L177-L193). |
| Guarded lifecycle changes | Storage tests cover stale restoration, space boundaries and preserved reclassification across backends. These are substantive test definitions, not executions by this review. [Entity lifecycle tests](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/storage/src/entity_repository.rs#L3110-L3173). |
| Audit-based reversal | Reverts supported create/update/delete/restore actions. Promote, merge and purge are deliberately non-revertible. [Public contract](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/docs/api/system.mdx#L685-L695), [MCP test](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/mcp/src/handlers/system.rs#L2136-L2215). |
| Temporal correction and supersession | Truth-maintenance tests cover deleted-claim recomputation and preserving adjudicated supersession against reopening. [Recomputation](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/pipeline/src/truth_maintenance/mod.rs#L8918-L8952), [supersession guard](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/pipeline/src/truth_maintenance/mod.rs#L10834-L10901). |
| Permanent deletion and retention | Space-scoped hard-delete primitives exist on both backends. Retention is enabled by default and its worker is started by the pipeline. [Entity hard deletion](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/storage/src/entity_repository.rs#L1718), [retention startup](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/bin/elephant-pipeline/src/main.rs#L7624-L7666). |
| Work-history recovery | Terminal work is archived, retained and retryable; snapshot tests preserve payload, classification and provenance. [Archive/retry](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/storage/src/work_retention.rs#L717-L806), [snapshot coverage](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/storage/src/work_retention.rs#L942-L1017). |

## What these mechanisms do not establish

Per-record hard deletion is not a person/source-wide dependency traversal. The current retention worker enumerates `doc`, `doc_chunk`, `entity`, `fact`, `rel` and `source`, followed by age-based audit/system-event cleanup. It does not enumerate the current revision, node, assertion, attribute, vector, work-artifact and view families as a coordinated erasure operation. The embedded branch skips `doc_chunk` and `fact`. [Exact cleanup inventory](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/crates/pipeline/src/retention.rs#L37-L84).

Recovery deliberately retains data and can make work runnable again. Erasure must therefore coordinate with retained history, queued attempts, source re-ingestion and old-backup restoration. Existing tests for a disappearing row or repaired fake vectors do not establish those combined guarantees. The [domain documentation](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/docs/architecture/domain-model.mdx#L135-L140) already distinguishes soft deletion from erasure.

The source-scoped erasure and restore-journal contract in [PLAN.md](PLAN.md) remains the recommendation supported by both consumers and required for OB3 real-data adoption. It should build on the existing lifecycle machinery. Broader person discovery/erasure remains a separately bounded decision, not an assumed removal of existing functionality.

## Historical evidence and limits

The current repository's reachable Git history starts with the September source import. A separate preserved January-September history and ten archived worktrees were also inspected read-only. They contain substantial early delete/restore/audit-revert work, including the `Purge` audit action by January, later purge-related audit schema support, and documentation distinguishing permanent purge from soft deletion.

That retained evidence did not identify a person-targeted RTBF/GDPR executor, associated integrated test suite, or removal of such a feature. An audit enum is not an erasure executor. Work outside the inspected history, including an uncommitted or other-machine branch, remains possible. Do not restate this bounded result as "RTBF never existed" or as a legal-compliance verdict.

The [historical retention repair receipt](https://github.com/lukacf/meerkat-recall/blob/f59e6f5c99ed095a015656626f4d32c106c13075/docs/internal/retention-race-repair-evidence.md#L96-L130) records selected passing tests, with explicit source and scope limits. This follow-up inspected code and test definitions only. Neither that receipt nor this clarification qualifies a complete erasure guarantee or closes the audit's propagation and stale-work findings.
