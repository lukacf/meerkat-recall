# Original reprocessing admission proof

`source_publication::SourceAcceptanceRetry` distinguishes `NewIntent` from `Existing { operation, contact }`. An existing plain operation has `contact: None`, even when its immutable metadata contains a contact object. Only an originally stored server seal proves contact admission.

`SourcePublicationRepository::source_acceptance_retry` validates the original scoped operation and semantic request/input before returning its original receipt and seal. Erasing or erased physical sources refuse payload-bearing preflight. This read grants no new write authority and never changes the receipt's producer origin.

`prepared_reprocess_contact(request, original)` returns that existing admission before testing the old document's current publication fence, because the successful first admission has already advanced it. For a genuinely new intent, it requires the exact original publication to remain current and prepares fresh contact effects from the saved immutable source only when that publication admitted a contact capability. Plain publications remain plain. Callers still authorize every original old and proposed contact envelope, and actual acceptance applies its original CAS, producer, attempt and configuration guards.

The retained intake owner consumes this proof in ordinary contact preparation and ExtractDoc reprocessing. Both-backend regression source covers plain metadata-contact retry, sealed retry after head advancement and ambiguous selectors, and stale-original refusal for an unaccepted key. Tests are authored; central qualification owns execution.


A prepared reprocess producer retains every verified selected WorkArtifact, not just the index and currently admitted source. Its constructor takes the full verified intent slice as its fifth argument, validates closed count/unique scope/membership, and the source acceptance transaction compares every full row plus the complete live job-scoped source-intent ID set. M owns recipe and digest verification before construction; exact snapshots preserve that verification through commit.

Only `reprocess_selection/v1` and `reprocess_source_intent/v1` WorkArtifact mutations participate in canonical epochs. Kernel direct/transaction/raw-delete paths and migration 041 check both old and new kinds and both spaces on a move, covering concurrent additions/removals and kind changes. Ordinary extraction staging remains excluded to avoid self-invalidating publication plans. Source regression cases cover changed sibling intents despite a refreshed epoch and uncounted membership; kernel regression covers direct/transactional kind transitions, moves, hard deletion and rollback. These tests are authored, not locally executed.
