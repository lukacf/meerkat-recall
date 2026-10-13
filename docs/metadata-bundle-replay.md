# Metadata producer replay

An `ingest_metadata` attempt can save its successful `WorkRun` and candidate,
then lose queue acknowledgement during settlement or process failure. Lease
recovery may execute that producer again. Its metadata candidate has a stable
ID within each run; the metadata commit bundle has a stable ID within the
accepted source and configuration plan. Both original validators can therefore
reach the same bundle after different successful attempts.

The validator retains the selected bundle's original candidate and `WorkRun`.
A different attempt may reuse it only for `source_metadata`, after checking:

- Both successful, completed runs belong to the same original metadata work
  item, with distinct recorded attempt generations and valid run-local candidate
  IDs. The producer is complete and still carries the accepted source plan.
- Both candidates contain exactly the metadata derived from the immutable
  accepted source. Publication, source generation, revision, configuration and
  plan tags are checked explicitly. Two equally modified candidates do not
  establish equivalence.
- Security, subjects, subject state and provenance agree exactly. Candidate and
  bundle validation records must exist without errors or deletion.
- The selected bundle differs only in its authenticated candidate reference and
  producer run. Its stored bytes, selected candidate and original run are never
  rewritten to adopt the retry.

The commit payload and idempotency key use the selected original run. This
preserves continuation if the first validator stopped after creating a bundle
but before enqueuing its commit. Lookup includes the durable source and plan
suffix. An existing commit child's original validator predecessor is retained
and authenticated; replay never rewrites that dependency. A concurrent create loser reads and authenticates
the winner using the same proof; duplicate insertion alone grants no authority.
A retained `Committing` bundle remains pending until its owner finishes. It is
not treated as an empty successful commit.

Knowledge candidates and resolution patches retain their existing strict
conflict rules. The replay helper supplies no new publication authority: source
finalization still checks the exact selected candidate, producer, manifest,
current attempt, source generation and erasure barriers in the existing commit
transaction. A changed or erased source cannot be restored by metadata replay.

The native regression source exercises the saved-result interruption, actual
lease recovery, both validator arrival orders, deterministic duplicate creation,
selected states, changed metadata and provenance, invalid producer history,
configuration/generation changes, retired attempts and physical source erasure.
These are local correctness controls and do not constitute provider or consumer
acceptance.
