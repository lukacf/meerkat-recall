# Independent review of the Responses count proposal

Status: source contract review only, 2026-10-06. No count request, credential
inspection, model probe, paid authorization or installed-gateway execution.
The current gateway's 17 independent offline passes at
`41cf48fc7caa5d64106644861bbb03b4e44a1215` do not cover this proposed successor.

The [official token-count guide](https://developers.openai.com/api/docs/guides/token-counting)
describes complete Responses input counting, including tools and model framing.
The [endpoint reference](https://developers.openai.com/api/reference/resources/responses/subresources/input_tokens/methods/count)
documents `POST /v1/responses/input_tokens`, accepts encrypted reasoning items
and assistant phase, and returns `object` plus `input_tokens`. It supplies no
request digest or deployment identifier. This establishes a documented candidate
for complete input counting; account/model/cipher compatibility is untested.

The pinned SDK defaults to `store=false` and replays full inline reasoning,
including completed encrypted content. Governed dispatch clears provider
overrides. Stored `previous_response_id` continuation is optional, so it can
remain refused without stripping the actual default history. The current gateway
refuses encrypted replay; its serialized-byte allowance and framing constants
are not an independently established bound for that full context.
The SDK can also emit an optional `phase` on a reasoning item. The count reference
documents assistant-message phase, but not that reasoning-item field. This
conditional compatibility case must be explicitly tested or refused; it is not
permission to drop an unknown reasoning field or evidence that normal calls use it.

## Required contract before an independent candidate test

| Boundary | Independent expectation |
| --- | --- |
| Count input | Derive a versioned, reviewed projection from immutable, already transformed generation JSON; preserve all context-bearing fields, complete reasoning, assistant phase and tool schemas. Refuse unknown fields and mutable server references. |
| Completeness | `truncation=auto` cannot silently drop history. Incomplete reasoning items or a partial count HTTP response cannot authorize generation. |
| Binding | Persist both exact bodies and hashes, projection version, count, scope, credential generation, endpoint, model alias and reasoning configuration before forwarding. A count applies to exactly one immutable generation body. |
| Refusal | Missing, malformed, boolean, fractional, negative, over-limit or ambiguous counts must prevent generation. A failed count is not a zero-token result. |
| Admission | Recheck authorization, configuration, revocation and original deadline after counting and immediately before generation. No body mutation or cross-space/account reuse is allowed. |
| Physical calls | Reserve and capture the count request before send. Count retries consume additional slots; no hidden SDK retry or retry refund is allowed. A crash between count and generation cannot replay either call implicitly. |
| Outputs | Preserve the reviewed explicit output-cap transform, full bounded capture and frozen semantic thresholds. Truncation or incomplete generation fails qualification. |
| Evidence | Test installed SDK continuation separately from handcrafted JSON. Offline count fixtures qualify gateway behavior only; they cannot establish actual provider counting or semantic quality. |

Digest binding prevents local request mismatch. It does not make two provider
requests atomic or pin a mutable upstream model deployment between them. A future
report must state its reliance on provider-described accurate counting and retain
available response usage/model/request identifiers; detecting a discrepancy after
generation cannot undo incurred charges.

## Workload and authorization remain unresolved

One count for each of the proposed 280 generation attempts adds 280 physical
requests: **712 total**, including the unchanged 152 embedding requests.
Retries, request/capture storage and count latency need explicit finite bounds
within the run's concurrency and original deadline. Responses counting does not
establish the embedding input bound.

Count-endpoint billing and rate treatment have not been established. The proposed
generation/embedding reservation of $68.97467392 leaves $6.02532608 below $75;
that difference is not a justified count-charge allowance. A prospective charge
bound and explicit revised authorization are required before any count request.
No model entitlement, free endpoint, immutable model version or paid approval is
inferred from public documentation or the existence of a credential name.

The frozen corpus, expected evidence and quality thresholds remain unchanged.
G owns a concrete gateway successor; D owns its independent expectations and
verdict. No guessed product or gateway API is added by this review.

Parent requested an alternative that does not require counting. The separately
reviewed [full-context reservations](FULL_CONTEXT_PROVIDER_ALTERNATIVES.md) use
the documented model maximum and long-context rates for each physical attempt.
Count economics therefore need not block that more conservative candidate.
