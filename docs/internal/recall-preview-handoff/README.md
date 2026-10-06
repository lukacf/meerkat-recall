# Meerkat Recall preview: plan and implementation handoff

Start here when handing this work to a new agent on another machine. This package combines the static audit, the HomeCore and OB3 reviews, the proposed parallel delivery plan, and the existing source checkpoint. It is dated 6 October 2026.

**Ownership:** Luka explicitly requires a different agent on another machine to implement this plan. The existing `elephant-release` agent, from the **Elephant - cleanup** chat, must not execute or delegate the plan. It has stopped its F01 audit-remediation work and preserved a checkpoint. The destination agent/machine has not been selected in this document.

Publishing these documents does not start implementation, tests, deployment or consumer migration. The work in this audit chat remained static. A receiving agent needs the implementation mandate supplied with the handoff; the proposed packages are not assignments issued by this document.

## Read these four documents

| Document | Purpose |
| --- | --- |
| [Plan](PLAN.md) | Complete six-package proposal, sizes, risks, shared contracts, parallel branches and qualification gates. |
| [Audit](AUDIT.md) | Architecture/maturity review and F01-F11 findings with pinned source evidence. Findings describe their stated baseline, not every later revision. |
| [Source status and transfer](SOURCE-STATUS.md) | What is already merged, what remains, and the unqualified F01 work to recover rather than duplicate. |
| [F01 checkpoint](checkpoint/README.md) | Exact base, patch, checksum, staged-tree identity and known compilation failure. This is incomplete source, not a passing candidate. |

All necessary plan text and the source patch are in this GitHub directory. Local machine paths, private bus archives and this conversation are not prerequisites for reading the handoff. The repository is private; the receiving agent needs repository access.

## Human decisions to preserve

- Recall must be an optional Meerkat family member and independently usable. Core owns execution/session concerns; Meta means MobKit. Connect is a work in progress, not the maturity benchmark.
- **Spaces remain core.** Named spaces, authorized administration, per-space configuration, isolation and recovery are retained. A default-space-only preview was explicitly rejected.
- **No generic legacy migration or backward-compatibility program.** HomeCore, the existing real user, may own a bespoke transition. Existing useful aliases and finite migration safeguards need not be removed. Fresh initialization and retained save/restore commands must still behave honestly.
- Favor substantial independently executable assignments, separate worktrees/branches, and as much useful parallelism as possible. Wall-clock time and integration cost matter more than token scarcity.
- The six-package structure is the recommended execution design, not a blanket approval of every scope proposal. Proposed cuts such as one storage backend, wiki/views deferral, broader lifecycle restrictions or embedding-profile immutability must not silently become user decisions.
- Source-scoped erasure is the recommended bounded lifecycle addition. OB3 requires it before real-data adoption, and HomeCore supports it. This is not a claim that a general person-level purge engine is already selected or that a consumer's full adoption is a universal publication gate.
- HomeCore transition/observation and OB3 qualification proceed independently. Neither is automatically serialized behind the other.

## Suggested forwardable instruction

> Take ownership of the Recall preview work on this machine, using this directory as the handoff. Read README, PLAN, SOURCE-STATUS and AUDIT. Preserve the accepted human decisions and keep unselected scope proposals explicit. Recover the existing unqualified F01 checkpoint before assigning overlapping work. Agree the small shared contracts, then organize substantial independent worktrees/branches under the package owners. Do not assign implementation back to elephant-release. Keep product implementation, exact-artifact qualification and consumer production activation as separate claims; production actions require their own applicable authorization.

Use that instruction only when actually assigning implementation to the receiving agent. Sending the link for review alone does not select a machine or start work.

## First actions for the receiving implementation owner

1. **Confirm the assigned machine and scope.** Identify the new lead, package owners and consumer contacts. The bus aliases `homecore` and `ob3` identify the reviewed consumers; `elephant-release` is a source-transfer contact, not a plan executor. Use the user's applicable messaging authorization for cross-agent coordination.
2. **Resolve the source base.** Read current upstream and open work. Main was independently checked at `f59e6f5c99ed095a015656626f4d32c106c13075` for this publication. Do not use an old worktree or pretend this dated snapshot stays current. Reconcile later changes before applying the F01 checkpoint.
3. **Recover existing work without duplicating it.** Designate one checkpoint custodian before splitting packages: its 20 files span authorization, provenance and queue/retention, so the custodian coordinates acceptance with A/B/C rather than letting them independently rewrite it. The checkpoint has no successor commit or PR and failed compilation before tests ran. Inspect its manifest, preserve its original bytes, and review it as incomplete work. The known compiler error and source-review caveat are in the checkpoint README.
4. **Freeze the small shared contract.** A owns canonical publication, lineage, source-head generations and receipts. B owns attempts, recovery, delivery and restore barriers. C owns space/grant/provider configuration and public schemas. D and both consumers review concrete examples before independent branches diverge. This is a short integration step, not a repeat of the entire audit.
5. **Start independent packages concurrently.** D's oracles, E's HomeCore transition preparation, F's OB3 adapter/fixtures, C's packaging and B's operational groundwork can proceed against those contracts. Only integrated acceptance must wait for the common A/B/C join. Extra agents should have substantial owned artifacts or review scopes, not competing authority over the same write path.
6. **Keep coherent integration ownership.** Each package lead integrates its helpers. A single designated owner resolves shared schema/manifests and commit semantics. Use frequent coherent joins. A/B fencing and publication must be qualified together; moving conflicting branches under one label does not reduce integration risk.
7. **Verify the delivered artifact and report limits.** Final evidence names source revision, artifact hashes, backend/configuration, provider profile, executed/skipped checks and consumer status. A passing source test is not native/container acceptance; an authored fixture is not field adoption; a healthy process is not proof that a requested revision is indexed.

## Validation and authorization boundaries

This package was prepared from source, documentation, GitHub metadata, existing operator receipts and agent interviews. No product build, test, linter, service, database or model probe was executed by the audit/publishing chat. Lightweight checks of these documents do not qualify the product.

When implementation is separately assigned, follow the repository's current [agent guidance](../../../AGENTS.md), [contribution guidance](../../../CONTRIBUTING.md), [active architecture](../../active-architecture.md) and [non-active catalog](../../non-active-catalog.md). Preserve the repository's worktree/build isolation and required checks. Do not inherit the audit's static-only result as permission to skip implementation validation, and do not infer permission for paid providers, production access, migrations, releases or deployments merely from a planning link.

For every exposed capability, either implement and qualify the promised semantics or obtain an explicit scoped restriction and enforce it across adapters, direct dispatch and background work. Documentation, a preview label or hiding a tool is not enforcement. Spaces cannot be removed as a convenience cut.

## Questions still bounded but unresolved

The receiving implementation lead should resolve these from the adopted scope without restarting broad discovery:

- Exact public source/receipt/error schemas and operation completion semantics.
- Source erasure, backup retention and restore-time replay guarantees.
- Whether a populated space's embedding profile can change in the preview; if so, the reindex/activation contract.
- The supported quiesce/export command and health/readiness contract across native and container shapes.
- Comparable latency/output budgets and finite HomeCore observation criteria.
- Which proposed feature deferrals, if any, the user actually selects.

Independent useful work can continue while those choices are resolved; dependent behavior must not be invented silently. The [plan](PLAN.md) identifies the corresponding owners and risks.
