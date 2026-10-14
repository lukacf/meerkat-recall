# Published source progress

Source reads preserve the immutable published revision's content and security.
They project the existing mode, lifecycle and latest-decision fields from that
revision's original canonical completion, including while a newer acceptance is
pending. The current desired Source is still authorized, and the immutable
published Source is separately authorized. Existing source-head and envelope
read fences remain in place.

`SourcePublicationCompletion.published_progress` contains the achieved manifest
mode and a paired optional original IngestDecision ID/content fingerprint.
Missing progress is unknown historical proof; `Some` progress with both decision
fields absent certifies that the selected original metadata ancestry contained
no triage. An actual triage without its original decision metric cannot be
reinterpreted as decision absence.

Finalization follows the selected metadata bundle to its original candidate and
WorkRun, then traverses immutable producer origins. Child edges retain the
original parent work, run and attempt. Reprocess edges verify the original job
intent/handoff and inherited parent with the existing producer validators.
Cycles and more than 256 original nodes refuse. Successful triage supplies the
existing `WorkRun.metrics.custom["source_ingest_decision_id"]`, carried by the
runner from its actual triage result. No latest-decision query or new decision is
used. The selected decision must belong to the same named space and source and
match the manifest mode.

The prepared proof retains every observed artifact, work, run, job, original
private payload and decision. Both canonical transactions compare these exact
before-images before changing effects. Native checks retain typed values,
including absent fields, and take actual row/reference conflict writes; failure
rolls those writes back. The finalizer writes the certified decision link and
completed mode/lifecycle into its existing Source effect. The completion
certificate is committed with that effect, outbox, source head and receipt.
Planner recovery and embedding only load immutable plan/input identity: they do
not prematurely require a successful metadata producer, and cannot create a
completion from those partial requirements.

Reads resolve the published Source's original completion lookup, then verify
its exact immutable completion, original receipt/publication fence, manifest
digest, accepted snapshot fingerprint and completed operation. The current desired generation never
substitutes for the original completed generation. The actual decision must
still match its certificate. Missing or conflicting proof refuses projection;
there is no generic reconstruction of historical proof.

Regression source covers both storage backends: actual canonical publication
with certified decision absence; pending-revision content/progress isolation;
missing progress, changed mode, unpaired decision, changed accepted fingerprint
and writer refusal; actual original triage/child admission with an original
stored decision; unchanged native positive guards; changed decision, original
run metric and original attempt refusal with preceding-write and nonce rollback;
and actual canonical decision publication, pending retention and later decision
mutation refusal. These are storage controls, not provider qualification. The
runner owns real triage-dispatch metric controls, and the public-surface suite
owns actual Graph/Retrieval MCP/HTTP progress assertions. No local Cargo or test
execution was performed for this source checkpoint; central validation remains
required.

The source lookup is a copy of the completion payload under a distinct key in
the existing completion table, computed as
stable_key(["published-source-completion/v1", space, source_id]).
It is written with the immutable completion in the same canonical transaction.
A read uses it only to identify the original publication fence, then requires
whole typed equality with the immutable certificate before checking the original
receipt, manifest, operation and accepted revision. It is not a second source
head or independent authority. Ordinary maintenance and pending acceptance do
not move it; a successful later publication of the same revision replaces it
with that actual acceptance's completion. A Source input has no derived support
from itself. Later canonical Source effects have their own maintenance receipts;
those receipts do not identify the original completed publication.

The lookup keeps the existing explicit space/source columns, schema, reference
fences and physical erasure inventory. Missing original proof or lookup refuses
without historical reconstruction. Regression source also performs a real later
canonical Source effect, verifies its actual receipt and stored after-image while
the original completion lookup and projected progress remain unchanged, then
accepts a pending successor; rejects changed/missing
lookup and missing immutable certificate; and physically erases both completion
records through the actual source-erasure operation. Existing same-revision
reacceptance and late-fault tests now assert exact lookup replacement and
rollback to the previous original completion respectively.

Producer legitimacy is checked before terminal triage. An Unknown origin is
rejected, an original ReprocessJob is loaded and its real handoff verified, and
a terminal triage's own Child/inherited parent edge is verified and retained in
the transaction read set. The upstream coordinator may legitimately still be
running, so its edge is not replaced by a requirement that the coordinator has
finished. Added negatives retain the actual successful triage run and decision
while changing only its private origin to Unknown or an unproved reprocess job;
both new capture and the previously captured transactional guard must refuse.

A direct completed triage compares its retained payload attempt fence to the
original successful WorkRun metric fence and attempt number. It does not call
the live-claim verifier, which correctly requires Running work. Both backend
regressions also select metadata produced by the actual original direct triage
run, require Done with cleared lease fields, refuse changed attempt number,
missing fence and mismatched token, publish through the real canonical
transaction, and require the original Done work to remain byte-for-byte equal.

A Direct producer carrying the old public `_predecessor_work_item` hint has
unproved ancestry. Progress preparation refuses it instead of certifying decision
absence or selecting a latest parent run. The producer must retain the actual
original parent WorkRun and attempt through its private admission edge. Both
backend absence controls also admit and complete triage and metadata Work records
with that original public hint, retain an actual decision and successful runs,
and require refusal with unchanged head, operation, producer and original run,
and no completion certificate. Genuine independent metadata without a predecessor
still certifies absence through the existing positive controls.

Configuration-scoped triage fixtures put the exact original publication,
configuration and plan tags on both candidate and bundle before storing them.
The copied candidate payload retains those same tags. The production artifact
plan checks remain strict; successful direct triage cannot publish untagged
artifacts from a tagged producer. The maintenance fixture also enforces the
existing rule that a Source input never gains derived support from itself.
