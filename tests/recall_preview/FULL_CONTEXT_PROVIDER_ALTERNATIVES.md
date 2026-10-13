# Full-context reservation alternatives

These are **unselected, unauthorized proposals**, requested by parent at 23:30
on 2026-10-06. They preserve the 28 revisions, 24 queries, eight negative cases,
quality thresholds, both backends and original physical-attempt quotas. The
432-request/$75 v1 packet remains byte-for-byte unchanged. No provider call,
credential access, download or installed-gateway execution occurred.

The [official Sol model page](https://developers.openai.com/api/docs/models/gpt-6.1-sol)
documents a 1,050,000-token context and 128,000-token output maximum. The
[standard long-context prices](https://developers.openai.com/api/docs/pricing)
are $4 input, $5 cache writes and $15 output per million tokens. The
[Responses reference](https://developers.openai.com/api/reference/python/resources/responses/methods/create)
includes reasoning within `max_output_tokens`. Facts were rechecked for this
proposal; the old catalog's short-context prices cannot price this alternative.

Reserve the entire documented context at the higher cache-write rate **before
every physical generation attempt**, plus the selected output ceiling at the
long-context output rate. Do not subtract output from the context reservation.
This deliberately overestimates combinations the provider may not admit. It
requires no count endpoint, ciphertext token inference or local framing bound.
It depends on the provider enforcing its published input/output and billing
contract; it is not a guarantee against provider defects or changed prices.

| Explicit candidate | Output behavior | Per generation | 280 generations + 152 embeddings | Proposed stop cap |
| --- | --- | ---: | ---: | ---: |
| Preserve reviewed transform | Existing explicit 16,384 cap; record requested/forwarded bytes | $5.49576 | $1,538.97467392 | $1,540 |
| Preserve requests through 64,000 | Preserve requested cap; reject higher caps | $6.21 | $1,738.96187392 | $1,740 |
| Preserve requests through model maximum | Preserve requested cap through 128,000 | $7.17 | $2,007.76187392 | $2,010 |

No candidate increases an actual request's output cap. The first retains the
previously reviewed opt-in transformation; the others require a separately
reviewed gateway mode and preserve the extractor's current 64,000 request.
Truncation still fails semantic acceptance. Embedding reservations remain
8,192 total input tokens per physical batch at $0.13/million, conservatively
using the larger profile's rate for all 152 attempts: $0.16187392 total.

Every retry and continuation consumes a new original role/global slot and the
whole reservation. No refund follows a small response, timeout, refusal or
unknown usage. The original 432 sends, two concurrent dispatches, 5,400-second
run, 120-second model deadline, response capture bounds and synthetic corpus
remain unchanged. A larger monetary cap does not grant extra retries or calls.
Standard, nonregional requests only; no hosted paid tools or other service tier.
Taxes and unrelated account charges are outside this token-reservation estimate.

The gateway must still validate and preserve the actual full inline SDK history,
including completed encrypted reasoning whose original capture belongs to the
same run/backend/space/role/model/account scope. Captured ciphertext proves
provenance, not its token length. Missing/foreign/partial provenance refuses.
Server-side references, compaction, remote resources, paid hosted tools and
unknown shapes remain refused unless separately reviewed. No automatic history
truncation is permitted; a provider context-limit failure remains a failed case.
Actual SDK compatibility and bounded capture must be independently exercised.

Durable reservation before send, current approval/configuration/credential checks,
original deadlines, unknown-send halt and strict byte/framing custody remain
required. Public facts do not establish account entitlement or immutable model
deployment. Recheck prices and limits before a final exact-artifact spending
request. These source-only arithmetic checks cannot clear those prerequisites.

The [counted alternative](COUNTED_GATEWAY_REVIEW.md) is less conservative: one
count per generation would total 712 physical requests. Its count billing bound
is still unknown. Counting need not block implementing the full-context option;
neither option is authorized to run.

Parent's later 23:36 proposal adds a distinct
[strict terminal-settlement mode](TERMINAL_SETTLEMENT_REVIEW.md): reserve the
full worst case, then release only unused liability after durable validated
terminal usage. It may retain a $75 cap without promising full-run completion.
That new ledger policy is not implemented or qualified by this arithmetic file.

Machine proposal: [full-context-provider-alternatives.json](full-context-provider-alternatives.json).
Offline arithmetic and negative cases:

```sh
python3 scripts/qualification/recall_preview_provider_plan.py \
  --full-context-alternatives tests/recall_preview/full-context-provider-alternatives.json
python3 -m unittest discover -s tests/recall_preview -p 'test_*.py'
```
