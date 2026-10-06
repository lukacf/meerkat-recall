# Source status and transfer

Status recorded on 6 October 2026. This separates independently checked GitHub facts, earlier static findings and source-owner reports. Recheck upstream before implementation; this file is not a live release dashboard.

## Revision map

| Revision | Meaning and evidence |
| --- | --- |
| `7f8446856b3a9eb7b74976a406536e90d09c1cf6` | Original static audit baseline. [Audit](AUDIT.md) references remain pinned here unless explicitly marked otherwise. |
| `5119f0597753dfb68206111e9fd9aad457be6802` | Merged [PR #21](https://github.com/lukacf/meerkat-recall/pull/21), replacing PDF text extraction. That one-commit delta was inspected statically. It does not close the canonical trust/recovery findings and does not add OCR. |
| `dd2d96672e869e2c2b1247f703312b5e0f9637af` | Inspected head of [PR #22](https://github.com/lukacf/meerkat-recall/pull/22): Core 0.8.51, C1/C2/C3 and native-product work. Historical candidate, not current main. |
| `f59e6f5c99ed095a015656626f4d32c106c13075` | GitHub independently confirmed PR22 merged here, and main matched this revision when this handoff was prepared. This is also the F01 checkpoint's committed base. |
| `bb4d3d563301f489871825daf1097b20ff4e3d87` | F01 checkpoint staged tree, reported by its source owner. It is a tree identity, not a published commit or qualified candidate. See [checkpoint](checkpoint/README.md). |

## Reuse the completed work

PR22 includes useful shared HTTP/MCP caller verification, ordinary entity-envelope protection/CAS, retention retry/archive repairs, migration preflight, native executable/version/alias, installation/provenance and bounded CLI readiness work. The PDF replacement and its patched vendored source/notices are retained. Do not assign these pieces from scratch merely because the initial audit predates them.

The source owner's retained receipt records 85 selected deterministic cases passing with zero failures on `dd2d966`; live/paid-model scenarios were excluded. It also reports all eight hosted PR22 checks passing. GitHub merge status was checked independently, but the audit/publishing chat did not rerun those checks. Those results do not establish semantic quality, every failure boundary, exact native/container acceptance or consumer adoption.

Ordinary entity CAS is not source-head CAS. Token verification is not complete resource authorization. A CLI readiness check is not full service qualification. Migration preflight is not an atomic canonical commit, restore proof or general migration obligation.

## F01 checkpoint: stopped, incomplete and transferable

The existing agent implemented a draft authorization batch in its local branch `codex/recall-authorization-surface`. On Luka's direct ownership correction, it stopped and preserved 20 staged files. It reports no successor commit, push or PR and no remaining owned F01 qualification process. The branch was not present on GitHub when checked for this handoff.

Its qualification ended with exit 101 during compilation: `DocRevision` does not implement `HasSecurity` at `crates/mcp/src/handlers/system.rs:1990`. **No tests executed.** The owner identifies the API chunk handler as another call site for the receiving owner to assess. A final create-space preflight question was raised but not implemented or re-reviewed; it is a review note, not a confirmed new defect.

The owner reports four earlier independent-review issues fixed and re-reviewed clear, plus formatting/surface/version/docs checks passed. Preserve that limited evidence without describing the checkpoint as passing, merged or release-ready.

The byte-identical [unqualified patch](checkpoint/f01-unqualified.patch) and a portable [manifest](checkpoint/manifest.json) are included in this directory. This is a transfer artifact, not application of the patch to the repository. The publication changes only handoff documentation/artifacts. It does not repair the compiler error or execute the code.

## Finding disposition after PR22 and the stopped draft

| Finding | Current disposition for assignment |
| --- | --- |
| F01: authorization | PR22 provides partial repairs. Additional authorization source is in the incomplete F01 checkpoint. Recover and finish its qualification before deciding residual work; do not declare closure from the draft's existence. |
| F02: classified lineage | Remaining on the reviewed candidate. Extracted entity creation and missing/changed-source publication are distinct from ordinary entity-update CAS. |
| F03: grounding | Remaining. Preserve the barrier between unsupported interpretation and evidence-backed promotion/approval. |
| F04: canonical commit/retry | Remaining. Complete canonical effects, outbox and receipt must agree. |
| F05: leases/recovery | Remaining. Quiet named spaces and stale attempt publication need explicit recovery/fencing semantics. |
| F06: propagation/lifecycle | Remaining. Success-aware delivery/checkpoints and source visibility semantics are still required for retained behavior. No wiki/lifecycle feature cut is assumed. |
| F07: snapshots/configuration | Remaining. Enumeration/preflight work does not repair snapshot replacement or split save/load configuration. |
| F08: initialization/transfer | Partial: C2 repairs/bounds transfer preflight. Earlier schema statement handling remains. General legacy migration/backcompat was removed from requirements. |
| F09: semantic qualification | Remaining. The golden evaluator is unchanged; deterministic passes are separate evidence for their selected cases. |
| F10: independent product | Partial: naming, CLI/readiness and source packaging advanced. Supported service contracts, exact artifacts and independent consumer acceptance remain. |
| F11: accounting/retry | Remaining. Active-runner usage and failure semantics need qualification; helper fixes do not establish active-path correctness. |

This disposition reconciles existing reviews and receipts. It is not a new complete audit of later source. Challenge any stale mapping using the successor revision and evidence, not a generic green CI claim.

## New consumer contracts are additional work

The [lifecycle follow-up](LIFECYCLE-STATUS.md), statically checked at the same `f59e6f5` main revision, credits extensive existing delete/restore, audit-revert, supersession/reprocessing and retention/archive behavior. It withdraws the broad correction/restoration deferral proposal. Coordinated source/person erasure remains a separate coverage gap to assess. This clarification does not close F06 or claim tests were executed.

The HomeCore/OB3 discussion specifies source-head generation/CAS at acceptance and publication, revision-aware coverage/results, all-model-operation approved provider binding, and a recommended source-scoped erasure/restore barrier. These are not supplied by PR22's entity CAS or the F01 authorization draft. Their owners and scope distinctions are in the [plan](PLAN.md).

HomeCore needs MCP/native deployment and a bespoke optional transition. OB3 needs HTTP/container deployment and a separate consumer adapter. OB3 real-data adoption requires source-scoped erasure; synthetic qualification can proceed earlier. Neither consumer's full adoption is silently made a universal publication gate.

The old agent's F01-F11 implementation assignment has been withdrawn. The new machine/agent owns future plan execution only when assigned by Luka. The old agent remains a source/evidence contact.
