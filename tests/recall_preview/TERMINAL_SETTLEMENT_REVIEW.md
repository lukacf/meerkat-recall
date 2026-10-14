# Full reservation with terminal usage settlement

Status: independent source contract, 2026-10-06. Parent authorized G to implement
a **separate versioned policy** at 23:43, following the 23:36 proposal and D's
initial review. No paid execution is authorized. The current nonrefundable
gateway, its 17 independent offline passes and the original v1 packet remain
unchanged. This policy does not change product EUR accounting.

## Provider contract and conservative arithmetic

Use the [full-context model and price facts](FULL_CONTEXT_PROVIDER_ALTERNATIVES.md):
1,050,000 input tokens reserved at $5/million and a separately selected output
ceiling at $15/million, standard nonregional service only. Reserve both maxima
independently before every physical send, including retries and continuations.
The [Responses reference](https://developers.openai.com/api/reference/python/resources/responses/methods/create)
defines input/output/total usage, cached/cache-write input details and reasoning
output details; the output cap includes visible and reasoning tokens. The
[prompt-cache guide](https://developers.openai.com/api/docs/guides/prompt-caching)
states that cache-write pricing replaces the applicable input rate, rather than
adding another fee, and describes caching the full rendered context including
provider instructions. These are published provider semantics, not independent
account billing measurements.

For a strictly accepted terminal usage record with totals `I` and `O`, the
settled conservative charge is `A = 5000*I + 15000*O` integer nano-USD. Price
**every** input token at the cache-write maximum and **every** output token at
the long-context output maximum. Never discount cached tokens or subtract
reasoning; never add them a second time. Embeddings can retain their existing
small nonrefundable reservation, avoiding another settlement schema.

This relies on all billed model input/reasoning being represented by those
documented totals, applicable rates remaining within the pinned rate table and
no separately billed tools/services. Reject hosted tools, media, background
processing, compaction, nonstandard tiers and unreviewed charge-bearing fields.
Provider errors, undocumented charges, changed pricing, taxes and unrelated
account charges are outside this token-accounting guarantee. Reverify the
official limits/rates before final approval; preserve eventual usage/billing
reconciliation as separate evidence. No credential or account call is needed
to author these controls.

## Settlement admission

The old `transport.usage_from_response` is **advisory only**. It accepts failed
or incomplete terminals, defaults absent output to zero and accepts alternate
usage names. It cannot authorize releasing any reservation.

A new strict validator must receive the original immutable dispatch record and
the complete, durably captured response. All conditions below are required:

1. A successful 2xx response with complete HTTP framing, within the original
   deadline and byte bound. Exact original/forwarded request and response bytes,
   hashes, status and transport completion evidence must be persisted and fsynced
   before settlement. An SSE completion event alone is insufficient.
2. Strict finite JSON without duplicate keys. JSON responses must have one
   completed Response object. SSE must parse every data event and end cleanly,
   with exactly one `response.completed` terminal, one consistent response ID,
   no later output, and no failed/incomplete/conflicting terminal. A partial
   stream, malformed suffix or mismatched event/Response status cannot settle.
3. The Response must match the dispatch's provider/model, selected standard tier,
   output cap and completed status, with no error or incomplete details. Pin the
   allowed returned-model identity mapping; do not accept arbitrary model aliases
   by substring. Its capture is bound to this original run, account/credential
   generation, backend, space, role and request hashes by trusted gateway custody,
   not caller-supplied JSON or headers. A response ID cannot settle two attempts.
4. Require explicit integer `input_tokens`, `output_tokens` and `total_tokens`;
   reject booleans, floats, negatives and missing values. Require `I+O=total`,
   `I<=1,050,000` and `O<=original forwarded max_output_tokens`. Empty/malformed
   usage is not a zero charge. Require the currently reviewed detail objects:
   `input_tokens_details.{cached_tokens,cache_write_tokens}` and
   `output_tokens_details.reasoning_tokens`, all nonnegative integers, with
   cached plus written input at most `I` and reasoning at most `O`. No absent
   detail is silently invented as zero. Unknown usage fields or new accounting
   categories require review; otherwise retain the full reservation.
5. Compute `A` with checked integer arithmetic and require `0<=A<=R`, where `R`
   is this dispatch's original reservation. Inconsistent or over-reservation
   provider usage halts the run and preserves the original liability; reporting
   the anomaly cannot retroactively undo a charge.

The strict detail/identity schema is a candidate compatibility restriction.
Actual installed SDK responses must demonstrate it; mismatch retains the full
reservation and makes qualification incomplete. Never silently relax the schema
after observing a failure or infer spend from plaintext/ciphertext length.

## Durable accounting and crash rules

Maintain the invariant `settled_upper + unresolved_reservations <= run_cap`.
`settled_upper` never decreases. At admission, atomically reserve `R`, increment
the original role/global physical-attempt counters permanently, and enforce
the concurrency/deadline/configuration/approval limits before send. Larger input
reservations do not create extra attempts or increase output caps.

Settlement is a single durable transaction on the original dispatch. Compare
its immutable request/scope/rate/policy version, reservation and capture digest;
change its state from reserved to settled, add `A` to settled upper expenditure,
and remove `R` from unresolved liability. Only unused `R-A` becomes available.
This is not a refund of spent charges. Counts and original deadlines never reset.
Exactly repeated settlement is idempotent; a different amount, capture, scope or
rate for the same dispatch must halt/refuse without changing balances.

An error, refusal, incomplete response, missing usage, expired deadline, failed
capture fsync or unknown send retains `R`. A crash before durable settlement also
retains it; resume must not infer settlement from advisory counters or resend.
If a committed settlement's acknowledgement was lost, reread that exact durable
decision rather than applying it twice. Keep the original independent run anchor
and fail-closed ambiguity rules; deleting/recreating a ledger is not a new budget.
The capture and database ordering must never expose available money before the
evidence authorizing its release is durable.

A $75 cap can admit two full-context reservations at the proposed output caps.
Successful small responses could release unused capacity for later original
attempts. **It does not guarantee all 432 calls can complete within $75.** Failure
or large usage may exhaust the cap; the truthful outcome is incomplete
qualification, without reducing the corpus, dropping retries, changing semantic
thresholds or starting a fresh budget. No option is selected or approved here.

## Independent acceptance plan against G's concrete API

G owns the implementation and exact state/API names. D will bind source tests to
those names once supplied, with sockets forbidden and fake upstream bytes only:

| Boundary | Required positive and negative observations |
| --- | --- |
| Pre-send | Full-context reservation before first byte; two concurrent reservations cannot overdraw; exhausted cap refuses without send. |
| Strict usage | Complete JSON and SSE positives; missing/duplicate/nonfinite/boolean/fractional/negative fields, wrong totals/details/caps/model/ID/tier and incomplete terminals retain `R`. |
| Framing | Real stdlib HTTP parsing: short Content-Length, missing chunk terminator, ambiguous headers, trailing broken event and oversize capture never settle. |
| Arithmetic | Exact nano-USD totals; cache/reasoning subsets do not discount or double count; short-context rates and unreviewed versions refused. |
| Settlement | Exactly one release of unused capacity; repeated identical settlement is quiet; altered evidence and competing settlements cannot release twice. |
| Failure ordering | Failure before/after raw-body fsync and before/after transaction commit leaves durable liability consistent; reopen never refunds ambiguity. |
| Counters | Successful settlement does not replenish call/role quotas or extend the original run deadline; retry always costs another original slot. |
| Isolation | Foreign run/account/credential/space/role capture cannot settle or authorize encrypted continuation; current revocation prevents the next physical send. |
| Default compatibility | Existing nonrefundable mode stays unchanged; choosing settlement requires its exact separately reviewed manifest/mode. |

These offline cases qualify gateway mechanics only. Exact installed isolation,
real SDK continuation and actual provider semantic acceptance remain separate.

Independent executable source is now `terminal_settlement_contract.py`, selecting
a separately supplied coherent gateway directory. Its fixtures do not import G's
expected results or tests. It uses real temporary SQLite/capture files and the
stdlib HTTP parser; all socket construction is refused. The 15 methods cover
exact full reservation/charge arithmetic, malformed usage and refusal, SSE suffix
and streamed-item consistency, truncated/ambiguous framing, fsync and late-capture
failure, transaction failure before/after actual settlement commit, duplicate
response IDs/changed capture, original quota and simultaneous last-budget
admission, exact SDK empty-summary projection/scope, current revocation and
ledger-version separation. Unknown capture must retain the original reserve.
At authorship these methods have not been executed; G's coherent implementation
checkpoint and source-hash-stable independent run are required.

G's coherent d7014e85f5e91a29d6f1ad4e4ddf005df27aa21f makes the reviewed
SDK projection version and actual client source SHA mandatory constructor
identity. D independently hashed `vendor/meerkat-openai-0.8.51/src/client.rs`
as `105229842f1a221277513653d5396c8e36a116384dde406182f2e173cdd0f8c3`
and pins those literal values in its fixture. The first attempted run met this
new schema without those fields: 15 methods ran, with20 subcase errors, and
no ledger acceptance established. The original log remains retained. This
fixture adaptation adds the mandatory identity; expectations are unchanged.

Independent run at D82ff6b3 against exact G
`d7014e85f5e91a29d6f1ad4e4ddf005df27aa21f`: all15 settlement methods passed
in2.742s, zero failures/errors/skips. The separate retained v1 suite passed17
methods in1.594s. Gateway source hashes were stable in both runs and all real
socket construction was forbidden. Logs and machine-readable receipts are
`qualification-terminal-settlement-offline-2` and `qualification-gateway-offline-3`
under the orchestration validation directory. The original failed API-adapter
run remains recorded; it is not counted as a pass. These results qualify the
tested offline accounting boundaries only, not provider billing, actual SDK
continuation, artifact execution, network isolation or spending authorization.
