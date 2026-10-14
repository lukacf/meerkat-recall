# Citation route identity and response layers

This additive successor requires **`SearchEvidence.doc_id`**, alongside the
existing eight citation fields. It is the actual authorized parent Doc's record
identifier, serialized as `doc:` followed by its lowercase hyphenated UUID:
exactly **40 ASCII bytes**. The existing `internal_doc_revision_id` is
`doc_revision:` plus its UUID, exactly 49 ASCII bytes. Neither identifier is the
external `source_id`, and neither may be derived from an external source key or
another identifier's suffix. Claims, entities and chunks use the same resolved
original Doc and immutable revision under the existing read and authorization
fences. The additional `doc_id` member costs 52 compact JSON bytes per citation,
including its separating comma; it is included in existing response budgets.

Use both returned identifiers, with URI path-component encoding, to read:

```text
GET /v1/docs/{doc_id}/revisions/{internal_doc_revision_id}
X-Space-Id: <the original selected named space>
```

The route requires the current `read:doc` and `read:doc:raw` authority on both
records and current eligible lineage. Its exact link check requires the immutable
revision's `doc` to equal the supplied `doc_id`. It refuses absent, ineligible,
superseded, retracted or erased evidence, and refuses a changed original read
with `503 not_ready`. A still-published revision can remain eligible while a
replacement is pending. The record returned is the complete `DocRevision` DTO,
with flattened base fields: **`id` is top-level**, with no `base` wrapper.
`doc` is a record-ID string, not a nested object. `text` is the complete rendered
text; `text_hash` is its **bare 64-character lowercase hexadecimal SHA-256**,
without a `sha256:` prefix. `renderer` has `name`, `version` and
`config_hash`, whose JSON value is explicitly `null` or a string. The current
default text transform uses `elephant.renderer.text`, `1.0.0`, `null`.

The full revision GET has its own lossless encoded-body bound of **12,065,536
bytes**. It returns `413 payload_too_large` instead of clipping text or dropping
DTO fields. This bound is separate from the search response bound.

Actual current response layers have these limits:

| Layer | Bound |
| --- | --- |
| HTTP search's complete `SearchResponse` JSON, including results, coverage, partial and escaping | 262,144 bytes |
| MCP serialized inner `ToolResponse`/JSON value, including its wrapper | 262,144 bytes |
| MCP outer `Content` text escaping, JSON-RPC and SSE framing | Can be larger; the two product bounds above do not cap this outer representation at 262,144 bytes |
| Original qualification collector's complete wire allowance | 270,336 bytes; a collector limit, not a new product guarantee |

The HTTP/MCP shared search computes the selected response budget; the API MCP
adapter additionally checks the inner tool value before `Content::json` wraps
it. No quote or evidence is truncated. A consumer may choose one new limit
20→10 request after `payload_too_large`, preserving query, filters and revision
constraints. The original request still has `retryable:false`. A framed-wire
collector may conservatively refuse a larger response but cannot attribute its
own threshold to the server's outer protocol contract.

Rendered-only verification proves the returned revision's full text hash and
selected rendered slice/hash. It cannot independently establish that the slice
maps to the correct occurrence in a different original raw input. That stronger
check needs the exact pushed raw revision, or access to those same immutable raw
bytes under a verified binding, plus the pinned renderer and mapping algorithm.
A raw hash without the raw bytes cannot compute that mapping. Equal quotes alone
cannot disambiguate repeated occurrences. Recall retains its current original
source/publication/mapping proof even when a consumer does only rendered checks.

[The companion literals](search-evidence-route-v3.json) preserve the corrected
24-byte June example and its complete 25-byte rendered text. Its revision object
is explicitly an expected-field projection, not a fabricated complete response.
The historical examples, fourteen-case OB3 packet, corrected v2 packet and seven
independent coordinate cases are unchanged. The actual HTTP oracle now constructs
the GET path from the returned citation identifiers and compares both to its
original independently retained fixture identities.
