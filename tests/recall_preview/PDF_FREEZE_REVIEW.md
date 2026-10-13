# Independent PDF expectation freeze

D reviewed G source `50caf22aa76661a8152cb0d19ba60482cb837c1a` and license
correction `1f1437001c965b041ec3080fb14083e074fa11d5` before any parser run.
The target is `elephant-mcp --test recall_preview_pdf`: 13 parser cases,
one membership/hash case and two active intake matrix tests. Authorship belongs
to G; D independently reviews expected outcomes. All Rust cases are unexecuted.

Frozen files under `tests/recall_preview_semantics/pdf_acceptance`:

| File | SHA256 |
| --- | --- |
| `manifest.json` | `3e1e107671826ee4cadcf8ba2df8ded0fa68c6b005237cc87e66269d0b491f1a` |
| `expected.json` | `d5083926e49ca0cf5f7d46048ce11ad2079a00262d8d482b407e8f2af276cbdf` |

D verified all 13 actual byte lengths/digests against this fixed manifest,
read the generator and literal expectations, and independently decoded the
synthetic ToUnicode scalar mappings and Tj operands to the exact authored
Greek/Cyrillic/CJK lines. This is a static fixture audit, not parser execution.
No expected text is derived from actual parser output.

Expected outcomes preserve accented Latin; explicit Unicode mapping; positioned
page/footer order; literal markup; nested/repeated/inherited Form text and
multiplicity; simple AcroForm field names/values; existing invisible OCR text;
and CropBox exclusion. Image-only and valid blank PDFs return empty text, with
no invented metadata, image placeholders or OCR. Non-PDF, unfinished catalog
and corrupt later stream cases require errors; the complete first page cannot
turn an incomplete document into successful partial intake.

Only CRLF normalization and outer whitespace trimming are allowed when comparing
passages. Internal whitespace, Unicode, punctuation, line order and multiplicity
remain exact. Intake separately compares the exact unnormalized parser bytes
with staged preview and immutable accepted input, plus the original PDF digest,
full default security/subject envelope, actual queued work and unpublished state.
Malformed intake must leave no Source, Doc, operation or work. This wrapper uses
real handlers and non-bypass authorization on embedded storage, not signed HTTP.

The fixtures are original, tiny deterministic documents, with no borrowed font
programs or downloads. They do not establish arbitrary font rendering, bidi,
complex shaping, general layout, OCR accuracy, XFA/JavaScript/encryption,
malformation/security coverage or resource-limit behavior. Existing parser
resource-bound tests remain independently necessary. Final installed native and
container artifacts must reuse these hashes; source parser tests cannot qualify
those artifacts or full worker/model publication.

A failure requires source investigation. Do not regenerate answers, remove
cases or normalize away differing text merely to make the candidate pass.
