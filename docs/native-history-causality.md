# Native historical erasure authority

Source erasure records the original intent and reference membership before logical
purge. Under the same maintenance reservation it then inspects every native
version in the exact namespace/database. `prepare_history_authority` projects
opaque causal commitments from that complete inspection, fsyncs their monotonic
union in the original journal, and binds the complete scoped fingerprint used by
the native physical copier. No raw source payload or private reference key is
written to this journal.

The projection reuses the ordered decision closure used by live source
invalidation and the canonical native writers used by transaction guards. A
causal seed requires the decision's actual native transaction version, exact
record identity, owning space and complete modeled after-image. The opaque proof
binds every actual native byte, including metadata beyond modeled fields.
Identical content in an earlier independent transaction does not qualify.

A seed can continue through contiguous native versions whose complete values
match after removing only a validated `canonical_read_nonce`. Native commit
order controls adjacency; timestamps may have been explicitly assigned. A
changed value, tombstone or independently supported publication ends the chain.
The source support fingerprint and original last-commit pointer determine the
independent boundary; an unknown independent pointer cannot certify a tainted
continuation. No content-only search jumps over a boundary.

Support fingerprints retain the canonical writer's existing representation:
the lowercase, hyphenated RFC 4122 UUIDv5 returned by `stable_key` over the
complete modeled row. Historical inspection validates that exact format and
compares any projected fingerprint with the payload; malformed UUIDs, other
versions or variants, and noncanonical spellings cannot establish a boundary.

Snapshots export only current records, and import changes transaction grouping.
Before export, `backup_history_causality` derives opaque invalidating-source sets
for every current record from complete history. It considers all source
identities represented by decisions, including sources not yet erased. Empty
sets are explicit coverage. The restore owner authenticates this map with the
exact backup digest, original journal and native scope, then durably binds it to
actual imported versions before erasure replay. Imported decisions participate
in the original dependency closure but never seed effects using coincidental
import transaction IDs. Authenticated imported effects seed later nonce copies.
A later export carries forward the original authenticated causes.

Backup inspection uses the dedicated `for_backup_inspection` constructor. It
validates the original journal and scope without inventing erasure membership.
An inspection context can compute version commitments but cannot bind or decode
a physical erasure authority; those paths still require original membership.

Versionless backends do not claim that all version-zero rows shared an original
transaction. Their exporter instead calls
`backup_versionless_history_causality` with the privileged maintenance client.
It reuses `SourceDecisionRepository::invalidation`, including independent and
contact survival policy, and verifies the resulting current before-images
against the same complete export inventory. A changed or missing inventory row
refuses publication. These semantic proofs can bind actual imported versions
when restoring into a versioned backend. A zero/zero row never receives a
persistent native-version identity merely because its content matches.

The signed imported-origin union uses the original reference-index writer lock
across read, validation, union, atomic publication and acknowledgment. The
optional `restore_origins_required` index flag is omitted when false, preserving
old empty index bytes. Once acknowledged, the restore helper's startup validation
makes missing or corrupt imported-origin state a readiness barrier. Source
reference and erasure-history updates preserve this acknowledgment.

The original restore marker is a compare-and-set barrier under that same writer
lock. Beginning another snapshot or digest cannot overwrite an incomplete
restore. Completion supplies its original marker and refuses to unlink a
different or missing barrier. Existing persistent-store ownership and the
maintenance guard cover the complete destructive restore; matching marker
contents alone do not grant concurrent restore ownership.

Historical fixtures include the native `id` field supplied by the document
writer. Canonical CONTENT projections intentionally omit that field and are not
complete persisted rows. Inspection requires the persisted identity to match
the decoded native key before any causal proof is considered.

Source regressions cover exact native seeds, independent identical history,
explicit decreasing timestamps, nonce copies, semantic and deletion boundaries,
unknown support pointers, transitive decisions, scope/identity mismatches,
import batching, imported-to-new decision causality, future erasure intents,
versionless behavior and exact live inverse capture. These are authored source
controls; installed physical erasure and runtime qualification are separate
orchestrator-owned checks.

Current outbox checkpoints retain their exact five-field native writer shape:
`id`, `space_id`, `consumer`, integer `seq`, and native datetime `updated_at`.
Both consumer acknowledgments and the reserved producer event floor use the
length-framed identity from `outbox_checkpoint_repository::key`. The general
source-control helper currently produces the same key for the same two inputs;
the identity-negative fixture therefore uses another consumer's valid key.
Retention still refuses
extra fields, missing or non-native timestamps, invalid sequences and mismatched
identities; historical checkpoint versions receive no current-row exemption.
The checkpoint regression reads actual native postimages from both existing
writers before applying the retained-control validator.
