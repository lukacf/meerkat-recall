# Source retraction and physical erasure

Source lifecycle operations use the accepted source head, an explicit generation,
and the complete authorization snapshot that the caller evaluated. A stale
generation, changed policy/subjects/version, or wrong space fails before an
independent erasure intent can be recorded. Retraction keeps immutable input for
an explicit guarded restore; erasure targets one physical source incarnation.
A deliberate recreation allocates a fresh physical ID. Old journal replay never
follows the external identity pointer to that replacement.

Document-origin retraction uses `retract_with_document_guards`. It checks every
originally authorized caller/current document, observed document absence, the
original Source head, and any observed publication manifest in the same lifecycle
transaction. A matching original operation returns its receipt before stale
observations are checked; this replay cannot create a new mutation.

Retraction records the exact preceding acceptance fence in its existing private
operation payload. This is the desired acceptance, including pending work, not a
claim of completed publication. Repeated retraction preserves that original
reference. The transaction compares the original operation payloads as well as
the head, so a concurrent receipt change cannot silently replace the prepared
lineage. Explicit restoration resolves retained document/contact authority using
that recorded acceptance and the original retracted head. An A-to-B-to-A revision
sequence refers to the latest actual A acceptance, never the first operation
stored with the reusable revision. Missing predecessor proof remains unknown.

`SourcePublicationRepository::erase_live` reserves `Erasing`, appends a synced,
payload-free intent, and obtains the exclusive storage maintenance barrier. It
then reserves shared write admission in the affected Space under that original
intent before reading heads or inventory. It replays the complete journal's
private membership projection, captures the checked guard revision, prepares the
physical/inverse closure, and fsyncs exact removed-identity membership. The purge
transaction compares the captured guard revision and canonical epoch, applies
payload changes, projects permanent membership, and releases only its own shared
reservation atomically. It persists the purged state and commits the final receipt. The outer barrier
persists that final receipt before returning success. Failure leaves storage not
ready and cleanup pending. `recover_pending_erasures` and `replay_erasure` use the
same physical cleanup before readiness after restart or restore.

Restoring an older backup can lose operation receipts created after that backup,
including the original erasure operation's polling history. The independent
journal preserves physical erasure authority, not an additional copy of normal
API history or authorization envelopes. A restored acceptance receipt for the
erased incarnation is marked superseded and loses its desired/published revision;
it cannot become recoverable publication work or current searchable content.
A backup predating the source need not acquire a synthetic source head or receipt.
Its journal still requires the physical incarnation and all previously erased
reference identities to remain fenced before ordinary readiness resumes.

The physical inventory includes source input/revisions, documents/revisions/nodes,
managed media records/files, every vector partition, source-only canonical rows,
staged artifacts, hot/cold work and runs, dependencies/deadletters, views and their
immutable revisions/dependency indexes, replay/decision receipts, and content
bearing audit/outbox rows. Traversal checks the exact owning space. Payload-free
head/authorization/operation controls, external identity pointers, and permanent
reference fences survive. Managed-file deletion uses generated asset IDs, never
paths from source content. Existing bounded snapshot retention remains responsible
for deleting historical backup generations.

Opaque extension references use complete native identities, including the table
and String/Number/Uuid key type. Cleanup recognizes native Things, complete
record-reference strings, object keys, declared `{tb|table, id}` objects, and
references inside decoded JSON strings. Similar key prefixes and ordinary prose
do not establish ownership. Embedded physical deletion keys remain separate from
this canonical reference identity. The pinned native normalizer computes
HMAC-SHA256 tokens bound to the original journal, Space, table, and typed native
ID. The independently durable private key and monotonic membership remain outside
ordinary database backups. Physical deletion membership uses identity-only tokens:
a reference inside a composite key does not itself become an erased identity.
Actual native postimage checks still visit nested references. Runtime, crash, and
paired-server qualification are required before claiming these source paths work.

A shared reservation blocks every ordinary managed/native payload create, update,
and delete in its Space, including operational rows outside the canonical graph
epoch. A trusted maintenance scope carries the admitted original intent; ordinary
query builders overwrite any caller-supplied owner with NONE. The database
reservation leaves other Spaces eligible to write. The current storage runtime
still drains all its producers during physical purge and durable checkpointing,
so erasure temporarily blocks ordinary activity across that runtime. There is no
timeout or automatic foreign-owner release. Recovery
resumes stored reservations before later intents and fails closed if original
journal custody is missing. Membership cannot become stale through an ownership
change between planning, independent fsync, and deletion.

Restore replays all retained membership in the Space before preparing inverse
writes and compares historical tokens against actual restored identities and
payloads. This removes an older artifact whose only reference names an erased
opaque record even when the backup predates the source and that record. Backups
predating the source and already-erased heads still reinstall historical fences.
Surviving independent records remain outside deletion membership.

Decisions involving multiple sources use causal inverse receipts. Removing any
input invalidates the decision and dependent later decisions in reverse order.
The lifecycle transaction applies the inverse, preserving subsequent independent
field edits, before computing the payload closure. Shared supported rows retain
their independently justified semantic fields while source provenance/evidence
is removed. Surviving support fingerprints change in that same transaction.
An output deleted by that causal inverse cannot become a shared survivor merely
because its ledger names both inputs. The exact typed inverse-deleted identities
remain in the physical closure and permanent reference fences, and every support
row for those deleted outputs is removed. Surviving independent baselines still
undergo the original semantic-reference checks; unresolved content is refused.
The two-backend merge regression obtains both heads through actual acceptance,
producer admission and completion, observes the real two-source merge receipt and
support ledger, and checks dependent-artifact deletion and B's preserved baseline.
Its source definition is not an execution result.
The private contact support pointer follows actual committed receipts. When a
receipt is removed, retraction and erasure recover the exact surviving input
pointer or commit an owner-verified continuation with only surviving facts.
Unchanged rows also require pointer repair when their original receipt is purged.
Every new continuation's exact native row is checked against the current erased
identities and historical journal membership before the purge can write it.
Continuation receipts receive no physical-retention exemption. Missing legacy
pointers remain unknown; matching content or timestamps cannot recreate lineage.
Ambiguous legacy mixed semantic content or a stale support proof fails cleanup;
it cannot be silently certified as erased or discarded with independent content.

`ErasureFault` names the source reservation, journal intent, shared admission
reservation, private membership fsync, in-transaction purge, post-purge, and
post-persistence boundaries. The regression source invokes the
public operation on both backends, checks stale authorization/CAS, source/space
isolation, later writes, shared baseline preservation, explicit reactivation,
fresh incarnation survival, and recovery at each boundary. Test definitions alone
do not establish an execution result. Process-kill/reopen, retained-backup replay,
finite physical backup cleanup, and exact artifact qualification are required
before claiming crash durability or consumer adoption readiness.
