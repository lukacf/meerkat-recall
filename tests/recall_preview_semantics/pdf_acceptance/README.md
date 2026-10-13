# PDF acceptance corpus — source authored, runtime unqualified

This small corpus exercises the exact final plain-text parser used by active
`IngestFileHandler`: vendored `pdf-inspector` 1.25.2,
`extractor::extract_text_mem`, with optional OCR/Python/PDFium disabled. It adds
frozen, separately reviewable input files and literal expected output to the
existing `test_pdf_text_extraction_*` unit coverage in MCP's docs handler.
It does not replace those unit tests or the vendor's resource-limit regressions.

`expected.json` was authored from synthetic source content and intended order,
before executing the parser. `generate.py` constructs PDF input bytes only; it
never runs a parser or derives a golden answer from observed output. `manifest.json`
binds exact fixture bytes and the expectation file. D owns independent review and
freeze of the expectation/manifest hashes before qualification execution. Changing
expected text to fit observed parser behavior requires an explicit reviewed scope
decision, not automatic golden regeneration.

The corpus includes WinAnsi accented text, explicit ToUnicode Greek/Cyrillic/CJK,
page order and numeric footers, literal markup, nested/repeated/inherited Form
XObjects, simple AcroForm field values, visible text beside an existing invisible
OCR layer, image-only and blank pages, CropBox exclusion, and three small malformed
inputs. The corrupt second-page case requires total failure rather than successful
partial extraction. No unbounded/deep-recursion or exploit reproduction fixture is
included. All inputs are original, small and deterministic; no dependencies,
downloads, external fonts, models or OCR engines are needed to regenerate them.

The corpus deliberately distinguishes **Form XObject text** from **AcroForm field
values**. A field name/value is extracted data; this is not a claim about arbitrary
widget appearance rendering or hidden fields. The multilingual fixture has an
explicit character map and no embedded font program, qualifying text decoding
only. Invisible existing text is evidence present in the PDF, not independently
verified OCR accuracy. The image-only fixture must yield empty text, with no image
placeholder or metadata promoted into page text. `expected.json` lists the broader
unsupported claims explicitly.

The new auto-discovered integration target is
`crates/mcp/tests/recall_preview_pdf.rs`. It verifies corpus hashes and each actual
parser result, preserving interior whitespace, Unicode and punctuation. Its
embedded-storage cases call the real public `IngestFileHandler` with a fixed ABAC
principal and no skip-auth, compare staged and immutable accepted text against
the same frozen passages, and require pending source publication with no canonical
Doc or Assertion. Parser errors must leave no Source, Doc or queued work. These
are trusted-context handler tests, not signed transport or installed-service tests.
They start no worker, provider, service or model. Empty extracted text is honestly
accepted as empty pending input; it is not a successful graph extraction verdict.

The parser is independent of the Recall storage backend. The active wrapper test
uses embedded storage only; native/remote Surreal adapter parity and actual installed
binary execution are separate centrally scheduled gates. L/S can use these exact
manifest-pinned PDFs in their installed-file ingestion driver. No passing parser
fixture alone qualifies document publication, retrieval or generated claims.

Source generation and lightweight reproducibility check:

```sh
python3 tests/recall_preview_semantics/pdf_acceptance/generate.py --write
python3 tests/recall_preview_semantics/pdf_acceptance/generate.py --check
```

The first command intentionally changes corpus source files; do not run it to mask
a failing acceptance result. The second performs no parser execution or file writes.
Rust execution remains centrally scheduled, never implied by a static check:

```sh
env CARGO_BUILD_JOBS=16 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
    BINARYEN_CORES=8 TLC_WORKERS=8 \
    chrt --idle 0 ionice -c3 ./scripts/repo-cargo test -p elephant-mcp \
    --test recall_preview_pdf --locked -- --test-threads=8
```

No Cargo command, parser run, service launch or live-provider request was used to
author this checkpoint. Actual results and exact candidate SHA must be recorded
by the execution owner; this document is not an execution receipt.

The active-intake oracle distinguishes a durable accepted document projection
from a published rendering. Acceptance may persist one exact pending Doc carrying
the original publication fence and deterministic revision-bound identity. The
oracle compares that row to the accepted preview, requires the normal current-read
guard and public get_doc to refuse it, and requires no rendered DocRevision or
assertions before worker execution. Malformed PDFs still require zero source,
document, operation, and work effects; parser corpus hashes and expected text are
unchanged. This distinction does not accept partial parsing or early publication.
