# Maintenance audit authority

Derived maintenance audits inherit the union of the affected record's trusted
before and after images and its observed dependency envelope. A shrinking
aggregate can remove source A from its current membership while its audit still
contains the earlier A-supported state. The audit retains A until the original
decision is invalidated. Unrelated records read by the same maintenance batch
do not become per-audit provenance.

The planning constructor and the shared canonical commit validator use the same
storage-owned derivation. Missing, foreign, recursive or ambiguous affected
authority fails before publication. Supplied audit snapshots must include their
identity, provenance, security and subject envelope; those restrictions must fit
within the trusted target union. An audit can describe an intermediate mutation
of a coalesced effect. Its domain payload is neither rewritten nor compared by
untyped floating-point JSON equality. Compact audits without snapshots still
inherit the full trusted target authority.

Coalescing retains only the original and final target images. Therefore an
intermediate-only source (A → A+B → A) is refused before publication, even when
B is an observed input to the decision. The audit payload is not rewritten and
B is not silently omitted. Supporting that composed transition would require
authenticated target history; neither a caller-supplied snapshot nor unrelated
decision inputs establish it. The current detection path collects one direct
decision per slot key before applying it; audited conflict resolution retains
the detected group's provenance, and stale/memo updates retain slot provenance.
This is a bounded review of those active writers, not a universal reachability
claim about arbitrary callers of the planning interface. A regression composes
the transition through that interface on both backends and requires unchanged
canonical epoch, target and absent audit after refusal.

The commit validator rejects missing or extra audit source attribution and any
weaker security or subject envelope. It permits stricter authorization. Different
policy domains cannot be joined. A source-free target remains source-free even
when the decision reads unrelated source-backed inputs.

This does not change the decision's complete causal source set, dependencies,
before/after inverse images, outbox, receipt, attempt fence or source lifecycle,
generation and published-revision checks. Withdrawing any required decision
input still invalidates the complete decision and its dependent effects on both
backends. Control-only reprocess transitions retain their dedicated validator;
independent public mutations retain their existing authorization contract.

The accompanying storage regressions cover before-only provenance, authorization
joins, missing and forged envelopes, create/delete/observed targets, coalesced
snapshots, atomic refusal, causal inverse preservation and stale-source replay
refusal on both backends. Pipeline regressions traverse the active audit
constructor. These are authored test sources; execution is a separate validation
step. This patch changes no evidence capture limit, representation, query
threshold, retained source payload or historical database state.
