# Finalizer readiness and canonical epochs

Whole-source publication checks its complete manifest before promotion and
checks it again inside the canonical transaction. The early native readiness
check participates in physical storage conflicts: it writes private read nonces
on the accepted source revision and original ingest decision. Recovery staging
also writes a private read nonce on the original source head. These writes do
not change source evidence, publication intent, policy or producer outcomes.

Previously, the native canonical epoch events treated those nonce writes as
semantic mutations. An independently preparing finalizer could therefore lose
its original epoch to another finalizer's readiness check. Repeating preparation
every five seconds did not guarantee progress while other pending sources kept
performing the same checks. Increasing retry counts or deadlines would not
remove that interference.

Migration 045 changes only the canonical epoch event comparisons:

- `source_head` excludes its existing `fence_nonce` and `canonical_read_nonce`.
- `source_revision` excludes only `canonical_read_nonce`.
- `ingest_decision` excludes only `canonical_read_nonce`.

All remaining fields participate in the comparison, including future fields
unknown to the current Rust types. Creation, deletion and changes of space still
invalidate the affected spaces. The original nonce writes and the physical and
reference guard events remain mandatory; a nonce does not grant new authority.
Earlier migration files and their receipt checksums remain unchanged. Existing
stores apply the new migration through the normal explicit migration command.

No preparation adopts a newer epoch after reading its inputs. Actual canonical
content or authority changes still refuse an earlier plan. Finalization retains
the original source generation, immutable revision, operation, configuration,
producer history, complete manifest and current claim checks. Incomplete bundles,
embedding work and indexes remain incomplete. Provider attempts, dependency
retry intervals, consumer deadlines and published completion requirements are
unchanged.

The dispatcher regression stages two accepted metadata sources through their
actual producer chain and claims both original finalizers. It repeatedly invokes
the production readiness transaction and recovery's idempotent document staging
between canonical preparation and commit, requiring the epoch, source heads,
operations, accepted documents and original claims to remain unchanged. Staging
uses the immutable accepted source snapshot and observes the document identity
derived from its original acceptance fence. The original prepared write must
still commit. A separate plan from
the same old epoch must fail after that real write, without an effect or receipt.
Both sources subsequently require actual dispatcher execution and atomic
publication under their original claims. The existing fourteen-finalizer
concurrency controls remain in place. These deterministic controls use no model
or embedding provider and do not stand in for an independent consumer run.
