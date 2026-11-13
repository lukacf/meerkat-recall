# Recall security hardening implementation plan

Status: implementation started 2026-11-13. This is a staged engineering plan,
not a claim that every listed capability is active or security-qualified.

## Delivery order

| Stage | Deliverable | Acceptance evidence | Size / dependencies |
| --- | --- | --- | --- |
| 1. Runtime stability | Repair reproduced executor lifetime defects; remove unnecessary native-row copies; investigate retained memory | Before/after regressions, native reference equivalence and limits, workspace gates, installed macOS crash and memory observations | M-L; production crash attribution remains open |
| 2. Boundary and deployment | Shared source response projection hides locations and private source metadata without raw scope; authenticated deployment examples and startup validation | REST/MCP read and mutation response tests with denied/raw principals; safe default and explicit development startup cases | M; preserve source acceptance and storage representations |
| 3. Erasure evidence | Release-specific matrix for logical cascade, native physical history/index removal, interrupted erase/restore and managed backup expiry | Execute each claimed backend/platform gate and retain identity, invocation and results; unexecuted claims stay labeled | M-L; native backend fixtures and installed-system window |
| 4. Credential lifecycle | Explicit legacy-token compatibility policy, current-grant checks, asymmetric verifier/key selection and bounded key rotation | REST/MCP parity, issuer/audience/algorithm/key confusion negatives, revoked/expired credentials, compatibility migration tests | L; preserve local ABAC and already-admitted operation semantics |
| 5. Audit accountability | Complete protected actor attribution and export; define externally anchored tamper evidence | All mutation entry points correlated to protected audit; ordered export, deletion/rollback detection, retention/privacy analysis | L; public receipts remain identity-free |
| 6. Data lifecycle and trust | Source quarantine enforced on retrieval and provider dispatch, optional live retention, explicit subject-access/erasure workflow | Cross-surface denial and revocation tests; expiry/cascade/replay; ambiguous/shared-subject review cases | L-XL; requires lifecycle and audit foundations |

Complete identity federation, universal information-flow control, automatic
cross-envelope contradiction discovery, and replacing the licensed storage engine
are separate architecture projects. They are not prerequisites for the bounded
hardening above. Existing current service grants, standing-watch delegation,
provider approval and source publication fences remain authoritative.

## Stage 1 changes

1. Reproduce ordinary stack async-driver lifetime/aliasing defects with compile
   checks. Tie each driver to its exclusive runner borrow. Retain the cancellation
   cleanup repair. Do not suppress the unresolved dependency advisory.
2. Add a borrowed physical-identity plus native-row reference extractor. Preserve
   exactly the virtual array's traversal depths, shared node/decoded-byte budget,
   HMAC domain, typed identity normalization, errors and complete reference set.
   Use it at row-copy sites. This removes temporary allocations; it does not by
   itself establish the cause of a production allocator abort.
3. Run deterministic nested/encoded/composite reference equivalence and limit
   regressions, concurrency stress with synthetic data, and lifecycle closure
   tests. Inspect remaining unsafe executor ownership and profile retained memory.
4. Keep each safety change separately reviewable and portable between the public
   branch and the installed consumer's pinned source baseline.

## Consumer test contract

The consumer receives an exact source commit/base, bundle digest, build recipe,
executed test results and limitations. The consumer owns its installed build,
release window and rollback. A candidate is first exercised against synthetic
inputs, then the existing real workload. Household content and credentials stay
with the consumer; shared diagnostics contain only sanitized call paths, operation
kinds, counters and process/build identity.

Record authenticated readiness separately from liveness, restart/abort counts,
operation completion/error rates, and RSS and physical footprint with explicit
units. Compare inventory/invalidation cycles and idle baselines on the same
workload. A short quiet period is not a crash-fix proof. Observe at least 24 hours
and repeated formerly problematic cycles before declaring installed stability;
continuing memory growth or a new abort keeps that acceptance criterion open.

Every stage includes code review, focused regressions, the applicable repository
gates, documentation updates, and a distinct consumer handoff when behavior
changes. No new production feature is enabled merely by adding it to this plan.
