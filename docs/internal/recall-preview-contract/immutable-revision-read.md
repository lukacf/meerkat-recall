# Exact immutable document revision read

`GET /v1/docs/{id}/revisions/{revision_id}` returns the full existing
`DocRevision` JSON object without a wrapper. Both identifiers accept their typed
record form or a UUID with the route's implied table; a different table is
invalid. The existing bearer, current grant, explicit `X-Space-Id` or permitted
signed space selection applies. This is a query available in read-only mode.

The selected named space must exist. The Doc and revision must belong to that
space and the revision must point to that exact Doc. Both records require
`read:doc` and `read:doc:raw`, including their own classification and subject
lineage. Current ancestor policies and the original completed-publication proof
are checked through the shared `ReadEligibility` reader. Exact original Doc and
revision values and the reader's original Source/head observations are checked
again after response assembly. This route does not infer a revision from Doc text
or a source head, and never normalizes the observed text to fit an expectation.

The direct response contains flattened BaseFields plus `doc`, `text`,
`text_hash`, `renderer`, optional `manifest` and `supersedes`, `status`, and
`rendered_at`. It has no private Source control or receipt payload. The response
is serialized losslessly into a bounded buffer of **12,065,536 encoded JSON
bytes**: six times the 2,000,000-byte source text limit plus 65,536 bytes of
envelope allowance. This permits worst-case JSON escaping for supported text;
it does not promise that arbitrary renderer/media metadata fits. An oversized
whole DTO returns 413 without truncation. Existing search and collector limits
remain independently smaller.

| Outcome | HTTP / `error_code` |
| --- | --- |
| Exact eligible revision | 200, full DTO |
| Invalid or wrong-table path identifier | 400 / `invalid_request` |
| Missing or invalid bearer | 401 / `unauthorized` |
| Denied selected space, operation or Doc/revision envelope | 403 / `forbidden_space` |
| Selected named space absent | 404 / `unknown_space` |
| Foreign/mismatched, absent, unpublished, superseded or unsupported revision | 404 / `not_found` |
| Original read observations changed | 503 / `not_ready`, retryable |
| Storage/encoding unavailable | 503 / `unavailable`, retryable |
| Whole encoded DTO exceeds the limit | 413 / `payload_too_large`, not retryable unchanged |

Errors use the existing `{error_code,message,retryable}` body. Retracted and
erased source evidence is unavailable. While desired R2 is pending, published R1
can remain readable under its original completed proof and current parent
policies. Once R2 actually publishes, R1 cannot borrow R2's completion. Retained
source-less documents use the existing active-render eligibility rule.

The source depends on the shared completed-render proof, including normalization
that differs from raw accepted input. Authored tests exercise both storage
backends, full DTO equality, raw capability and envelope refusals, typed/mismatched
IDs, complete response bounds, actual finalized R1, pending R2, completed R2,
tampered render fields, retraction and erasure. These are test source, not an
execution or provider qualification receipt. The real pipeline fixture uses
controlled local transport and never fabricates a successful publication.

Collectors add one read per inspected source using the actual pipeline revision
ID. Primary adds 28 reads per backend within its existing 8,192-request budget;
D28 preparation adds 50 across its phases within the existing 4,096-request
budget. Original provider allocations, capture limits, deadlines and measured
query sets are unchanged. A response over a collector's smaller bound is an
explicit unmet observation, not permission to truncate or increase that bound.
