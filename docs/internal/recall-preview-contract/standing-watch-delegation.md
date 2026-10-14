# Recurring source-watch authority

A watch registered by an authenticated named service is an explicit durable
**delegation**. Its original token authorizes registration only while that token
is valid. JWT expiry subsequently ends reuse of the bearer at ingress; it does
not silently expire an intentionally recurring watch. Ongoing work remains
bounded by the actual current named-space service grant, enabled Watch, current
configuration, and the original effective scopes, clearances, subject allowlist
and purposes. A broader later grant never expands that original ceiling.

`elephant_auth::standing_watch::StandingWatchAdmission` is the sole auth admission
object. It is non-serializable and has private fields. `verified` consumes the
actual `VerifiedCallerContext`, already attenuated policy `Principal`, and exact
current `SpaceGrant` observation. It rejects expired registration, management
bootstrap/global callers, mismatched or inactive grants, a broadened projected
principal, and missing Source/Doc read/write authority. It retains no bearer.
`principal()`, `origin()` and `registration_grant()` expose only the original
ceiling and provenance needed by the owning storage transaction.

Clearance order is significant: the policy engine selects the first clearance
for a matching policy. Every projected entry must fit that first signed entry;
a later duplicate cannot provide extra level, labels or handling authority.
Persist and compare the original ordered clearance list. Individual scope,
label, handling, subject and purpose sets may be compared without set order,
but do not sort or deduplicate the policy clearance list into a different ceiling.

HTTP/MCP and standing admission share `elephant_auth::projection`. Registration
first intersects the raw signed scopes with the observed grant, then applies the
existing Doc metadata/raw, Source raw and event/timeline aliases. Aliases cannot
bridge mismatched raw ingress spellings. Explicit clearance order is preserved;
legacy flat signed fields project exactly the existing `default` and
`tenant_policy_v1` entries, with trusted-context handling on the latter only.
No other policy or handling is inferred. Recurring attenuation projects the
current grant using the same aliases, then intersects the already projected
original ceiling. It does not re-project the result or restore an operation
missing from that original ceiling. Wildcards never create management scopes.

The separate `trusted_local` constructor is for explicit trusted embedding and
library entry points. Missing verified context, a scheduler actor string, a tool
profile, or a caller-supplied serialized marker cannot select trusted origin for
a public signed request. Missing historical service authority requires authorized
reconfiguration; there is no inferred migration or default unlimited delegation.

B owns the private durable registration and enqueue transaction representation;
I owns public registration/check and current Source/Doc/Watch preflight; A owns
original canonical source/document/watch/attempt guards and carries B's prepared
standing proof. C owns verified-context construction and the attenuation contract.
Any new private durable rows need E's physical-source-erasure closure before join.
This auth module alone neither persists authority nor authorizes a fetch/commit.
Its coherent integration group must include all those consumers and guards.

At registration, storage must compare the exact observed active grant, current
configuration, old/proposed Watch and Source/Doc rows in the same transaction
that persists the original ceiling and semantic watch intent. Enqueued work binds
that immutable delegation identity/digest; a later scheduler string cannot replace
it. A different caller's reconfiguration requires full old/new authorization and
an explicit new verified delegation, making original queued work stale.

Before every future fetch/dispatch, the durable owner reconstructs the original
ceiling and observes the current grant/configuration. The pure
`attenuate_current_grant` function narrows the stored scope ceiling; it issues no
admission or commit proof. Missing/inactive/mismatched grants and removal of a
required Source/Doc scope refuse work. The exact observations used in that attempt
must also be guarded by every committing mutation. A fresh periodic attempt may
observe a legitimately changed current grant, but an in-flight attempt must never
substitute a later snapshot to rescue an invalid original proof.

Source tests cover verified registration identity, each required operation scope,
wrong/revoked grants, every projected authority dimension, ingress expiration
versus recurring revocation, duplicate-policy ordering, real API scope/legacy
clearance projection, raw ingress intersection, removed alias authority, unchanged
ceilings under broader later grants, and separate local origin. Storage race/rollback, configure/check no-write-on-denial,
network-before-revocation, historical-missing-authority and physical erasure tests
belong to the composed integration group. This document makes no claim that those
consumers or tests have executed merely because the auth source is present.

MCP watch management preserves typed public refusals. Missing or invalid verified
registration authority is `unauthorized`; grant, scope, projected ceiling and
record-envelope denials are `forbidden_space`. Missing Source/Watch is
`not_found`, and a wrong-table Source identifier is `invalid_request`. Pure
old/new authorization failures are classified at that authorization call, without
parsing storage error strings. Transaction and storage failures retain their
original typed mapper. Missing watches no longer use a success-shaped tool
payload with a string error. All these refusals preserve the no-write behavior.
