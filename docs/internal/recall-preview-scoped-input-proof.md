# Scoped evidence input integrity

Public source intake preserves its producer-specific deduplication fingerprint.
`ingest_source_simple` hashes the complete staged JSON envelope; manual document
intake hashes title, text, sidecar and media fields. Neither contract promises
that `Source.hash` or `SourceRevision.content_hash` equals SHA-256 of text alone.

Scoped search verifies the precise accepted source snapshot supplying citation
coordinates through `completed_document_revision_is_current`. That existing
verifier requires the original completed manifest, document and rendered-row
fingerprints, full accepted-source fingerprint, and canonical publication
receipt under the original publication fence. Missing or changed proof fails
closed. Rendered text independently retains its byte digest check. The final
accepted-source, mapping, evidence and read-authorization rechecks remain.
No producer hash is rewritten, inferred, defaulted, or accepted via an alternate
hash interpretation. No durable schema or operation-state authority changes.

Regression source uses public `ingest_source_simple`, real queued work-kind
dispatch, original attempt/run receipts, actual renderer, governed loopback
embedding transport and canonical finalization on both active backends in two
named spaces. Its original literal sentence and expected 48-byte rendered text
are frozen before execution. Lexical and semantic search must expose the same
original citation and revision; semantic search must make one governed query
embedding request. Shape-valid spy vectors establish transport/index mechanics,
not relevance. Extraction is a fail-closed sentinel and generation is forbidden.

After real completion, explicit storage-corruption controls alter (a) accepted
raw template text while retaining both producer hashes, and (b) both hashes to
an equal uncommitted value. Search must expose no result, the completion must
remain unchanged, and restoring the exact original payload must restore the
positive evidence. These controls do not manufacture acceptance or completion.

New exact tests in `elephant-scenarios`, target `recall_preview_providers`:

- `scoped_input_hash::embedded_public_intake_hash_uses_original_completion_proof`
- `scoped_input_hash::surreal_public_intake_hash_uses_original_completion_proof`

The test reuses the existing `tests/e2e/src/pipeline_driver.rs` unchanged. Its
300-second elapsed deadline per space, real persisted retry delays, finite
64-pass bound and rejection of unfinished work remain. Including the module also
exposes its three existing driver self-tests; they are not newly requested runs.
No runtime result is claimed by this source checkpoint. Root owns prospective
scheduling, exact selected tests and the later installed consumer same-query run.
