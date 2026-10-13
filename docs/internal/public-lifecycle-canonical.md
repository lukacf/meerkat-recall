# Retained public lifecycle and canonical propagation

Public entity, relationship, assertion, domain-event and identity-candidate
writes prepare a `PublicMutation` before reading the authorized current row.
The final transaction compares the captured space epoch and complete before
image, then writes the record, classified audit snapshot, semantic outbox event
and replay receipt together through `CanonicalRepository`. Batch interfaces
retain their documented per-item outcomes; each successful item is atomic.
Multi-record corrections spanning policy domains emit a separately classified
event per policy inside the same transaction.

Ordinary replacements preserve identity, creation metadata, full security,
subjects and subject completeness. Deletes and restores compare the same
snapshot the caller authorized. Source-derived records capture current published
support, so ordinary restoration cannot revive a withdrawn or erased source.
Explicit public edits use the independent-mutation origin, preserving source
fences without relabeling caller edits as source-derived computation. Public
replay verifies the current retained receipt authority and refuses a
payload whose record has changed since that receipt.

Audit reversal authorizes both the current and proposed envelopes. It compares
the exact recorded after-image against the current row. Legacy delete receipts
without an after-image require unchanged before content and a current tombstone.
Promote, merge and purge remain non-reversible. Source/document audit inverses
use the accepted-source lifecycle and require explicit `intake` controls:
`expected_generation`, `idempotency_key`, and a new `revision_id` when accepting
historical input. MCP carries these under `intake`; REST carries these as query
parameters. A source inverse passes the original accepted fence to the queue.

Relationship evidence writes resolve an actual immutable document revision,
authorize its raw content, require a unique exact quote, and inherit that
revision's complete envelope. Predicate registration and evidence staging share
the relationship transaction. No invented revision ID becomes provenance.

The test source exercises public adapter faults at canonical effect/outbox/receipt
boundaries, replay, stale delete/restore, and audited reversal against both
backends. These tests have been authored; execution and full lifecycle acceptance
remain part of integrated qualification. Identity merge/reference rewrite has a separate prepared canonical adapter.
Attribution restamping uses staged replacements in the public mutation adapter.
Neither path is qualified merely by these source changes.
