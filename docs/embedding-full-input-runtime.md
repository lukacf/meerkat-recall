# Full-input embedding execution

The [explicit profile contract](embedding-preprocessing-contract.md) selects
`text-v1` or `text-full-v2` for the same governed query, document and reindex
provider. The separately versioned English BGE retrieval recipe is described in
the profile contract; it does not use full-input segmentation. Existing profiles
and receipts are unchanged. A new immutable profile
and the existing checked reindex transition are required to activate v2. Direct
`OpenAIConfig` still defaults to v1; callers opt in with `with_preprocessing`.
The local provider exposes the same explicit selector. Unknown selectors fail.

`provider::embedding_execution_recipe` exposes the settings for an already
resolved governed binding: its original provenance, exact vector partition,
physical batch bound, total HTTP attempts and applicable v2 segmentation
limits. `provider::approved_openai_config` constructs the actual remote
configuration from that same recipe. The local constructor and recipe share
the same batch limit. This inspection contains no secret or credential lookup
and grants no dispatch authority; the caller must resolve current approval.
HTTP attempts include the first send (three total), independently for each
logical request. Size fitting and subsequent segments still require their own
physical admission under the original cumulative budget.

For v1, OpenAI receives complete original strings. A request-size rejection can
split a batch into complete inputs, but an individual oversize input fails.
There is no character cap, prefix truncation or prefix-halving fallback.
FastEmbed counts the complete original encoding, including special tokens,
against its captured tokenizer limit and refuses overflow before inference.

V2 initially partitions each original string into consecutive UTF-8 segments
of at most 2048 bytes. No normalization, whitespace removal or suffix deletion
occurs. If the provider rejects a segment's size, both UTF-8 halves remain in
the pending sequence. Whole-request limits split complete batches; unindexed
input errors isolate inputs before fitting. Splitting is bounded to 16 levels
per initial segment and refuses when one complete Unicode scalar cannot fit.
All requests and retries use the original dispatch handle and current binding.

Each original input has a separate accumulator that requires contiguous,
non-overlapping coverage through its exact original byte length. Successful
segment vectors contribute `segment_bytes * component` to an f64 sum. The
sum is divided by the original byte length, then L2-normalized into the final
f32 vector. Missing/duplicate/foreign response indices, wrong vector counts or
dimensions, nonfinite components, zero vectors and a zero aggregate norm fail.
No result is returned unless all original inputs and segments succeed. An empty
batch returns an empty result. V2 rejects empty individual strings before any
dispatch, because their byte-weighted mean is undefined; v1 retains its own
complete-input behavior.

The pinned FastEmbed loader verifies the approved assets and supplies the safe
model/tokenizer limit. The provider captures that limit and disables truncation
on the actual inference tokenizer. A clone without padding counts the same
special-token encoding to plan complete v2 segments. Inference retains the
library's longest-in-batch padding. Each explicit physical batch acquires the
model mutex, then checks current authorization and durable observer admission
inside the executing blocking thread, after its queue wait. It rechecks
authorization after admission. Cancellation prevents a queued task from
starting inference; an already-started inference still records its actual
usage even if its caller drops. `embed` receives an explicit batch
size equal to that batch's length, preventing unobserved internal batch splits.
Successful inference reports measured unpadded input token counts, including
special tokens. Failures retain unknown usage; no invented zero usage permits
an additional retry. Remote responses retain the provider-reported usage and
the same observer accounting.

This behavior preserves the original source tree and every required Paragraph,
Chunk and Summary node. The 2 MB paragraph becomes multiple provider inputs
and one complete aggregate; no source content or tree identity is cut to fit.
Batching still groups many segments in each physical request. Approved request
and token budgets can stop processing; partial vectors are never returned as a
completed input. Segmentation and pooling define a new vector representation;
controlled-vector tests establish coverage/arithmetic, not model quality.

Source regression coverage includes the retained v1 whole-request test, a
replaced v1 oversize test that requires original complete input and explicit
failure, actual WordLevel special-token boundary/Unicode/no-partial-plan local
tests without ONNX, and independent provider tests and literal fixtures.
Tokenizer and controlled-vector coverage do not establish inference quality;
real model qualification remains a separate validation requirement.
