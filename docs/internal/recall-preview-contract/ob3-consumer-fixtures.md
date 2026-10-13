# OB3 consumer fixtures

The companion [fixture packet](ob3-consumer-fixtures.json) freezes consumer
behavior for `recall-preview-1`. It contains synthetic inputs and predicates over
actual responses, not runtime receipts. It is usable before an installed artifact
is available. Independent qualification must review the cases; actual consumer
and Recall execution remain separate gates.

Persist an explicit intent key, exact body and expected generation **before** a
PUT. Retry that same intent after uncertainty. A previously accepted key can
return a historical 202 after the head has moved; GET the current head after PUT.
A deliberate A → B → A return uses identical immutable A input, a new key and
the exact current B generation. Poll the new operation. When current desired A
matches an older published A, coverage still follows the new operation; readable
old evidence does not prove that new work completed. The packet also specifies
stale-generation and changed-immutable-input refusals.

Constrain search with the consumer's expected **external source ID → external
revision** map. A citation separately supplies `internal_doc_revision_id`, exact
half-open UTF-8 span and complete quote. There is no `evidence_id` wire field.
If the consumer needs a local citation key, derive it from the full tuple
`(space, source_id, revision, internal_doc_revision_id, start, end, quote)` using
unambiguous structured encoding. Keep the tuple with the key.

The whole selected search response, including coverage and JSON escaping, is
bounded to 262,144 bytes. `413 payload_too_large` permits one **consumer-chosen
new request** with limit 20 reduced to 10, preserving every other field. This
does not change `retryable:false` for the unchanged oversized request. A second
refusal takes the ordinary error fallback once. Do not trim quotes or claims,
change revision constraints, or recursively retry. Count size retries separately
from the logical read's single terminal evidence/fallback outcome.

The actual review/analysis worker path must invoke Recall for an outage fixture
to test fallback. A standalone search helper that no worker calls is insufficient.
Configured outage fixtures require a positive attempted-read count, completion
of every requested worker, one evidence/fallback outcome per logical read, and
unchanged POR failure/degradation counts against the no-Recall baseline. The
external OB3 run owns the data clone and original receipts. This packet neither
starts that run nor supplies a passing receipt. A missing consumer removal-event
seam leaves retract integration explicitly unmet.

Physical erasure and bounded backup retention remain the OB3 erasure gate.
Acceptance, search invisibility and an authorized tombstone alone are not
physical-deletion evidence. Durable journal and restore-barrier behavior remain
part of Recall; replay is recommended without replacing the specified gate.

Authority: [revision reacceptance](revision-reacceptance.md),
[complete citations and response limits](search-evidence-delta.md), and the
[base wire examples](examples.json). Full immutable-revision inspection is an
additive collector route and is not an OB3 consumer prerequisite.
