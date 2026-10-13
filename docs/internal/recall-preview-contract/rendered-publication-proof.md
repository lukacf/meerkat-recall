# Rendered publication proof

Accepted input text and rendered text are different representations. The default text renderer normalizes line endings and trailing whitespace and adds a final newline; media renderers can produce text from attachments. Public reads do not establish publication by comparing the rendered text with the raw manual template.

The original source finalizer captures the complete typed Doc and DocRevision rows and the exact immutable accepted SourceRevision control payload. Both storage backends check these captured inputs in the same canonical transaction that publishes the source head, operation, effects, receipt, and completion. The native comparison uses the complete typed storage projection with explicit absent fields; it does not compare incompatible SQL and Rust hashes. The completion records domain fingerprints for every mapped document and rendered revision, plus the accepted source snapshot fingerprint. Fingerprints ignore only the standard top-level `updated_at` bookkeeping field.

Reads bind the exact original revision and document mappings, original manifest and its digest, completion, canonical receipt and full original publication fence. They compare the full document, rendered revision, and accepted source snapshot with the completed fingerprints. Recomputing a text hash after changing bytes, renderer metadata, recipe provenance, media manifest, parent identity, or policy does not satisfy this proof. The caller separately checks current source lifecycle, published revision, and both current and accepted authorization envelopes.

A newer desired generation does not hide the older published revision while processing remains pending. This read path intentionally does not call the canonical mutation-replay accessor, which requires current generation authority. It reads the exact historical receipt and binds its original authority to the currently published revision instead.

Older completion payloads deserialize with empty fingerprint maps. They cannot authorize this read path; an explicitly authorized fresh reprocess must produce a new real completion. No recipe marker or historical content hash is promoted into a certificate.

Storage regression sources cover changes after finalizer preparation to raw Doc fields, rendered bytes with a matching new text hash, renderer/manifest/recipe/security fields, and the original accepted source control. Real renderer/publication/read and pending-generation cases belong to the worker-backed pipeline qualification; source formatting alone is not execution evidence.
