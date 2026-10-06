# Elephant PDF text compatibility patch

Source: Firecrawl [pdf-inspector 1.25.2](https://crates.io/crates/pdf-inspector/1.25.2),
MIT licensed, from upstream commit `ef52f77850b29189048797a48b24862251f54fe7`.
This directory contains the published crate, excluding Cargo's registry marker
and the upstream lockfile. Elephant's workspace lockfile controls dependencies.
The original license, manifest, source, and bundled decoding tables are retained.

Only `src/extractor/mod.rs`, `src/extractor/content_stream.rs`,
`src/extractor/xobjects.rs`, and `src/types.rs` differ from the published source:

- Route plain text extraction through the existing positioned extractor, including
  Form XObjects and invisible text already present in OCR text layers.
- Resolve inherited page XObject resources, with nearest-definition semantics
  and a bounded, cycle-safe parent walk.
- Fail plain extraction on page read failures or page/form resource limits,
  so a partial document is never reported as complete after those failures.
- Group lines without filtering page numbers; omit image placeholder items.
- Add an internal serializer that preserves spacing and literal text without
  generating superscript or subscript HTML. Existing formatted APIs retain
  their serialization and best-effort extraction behavior.

Elephant disables all optional features, including Python, OCR engines, and PDFium.
The patch extracts existing text; it does not perform OCR on scanned images.

Regression coverage lives in `crates/mcp/src/handlers/docs.rs` under
`test_pdf_text_extraction_*`. Before updating this copy, run those tests and compare
the four changed upstream files. Remove the workspace patch when an upstream version
passes the same tests through `extractor::extract_text_mem`.
