# Full-context terminal settlement: implementation contract

Source implementation is authorized by the orchestrator's 2026-10-06 23:43
instruction. This is a new qualification-only policy; it is not a live-run
approval. The original manifest, 432-attempt/$75 proposal, and nonrefundable
`Ledger` financial policy remain available unchanged. Its constructor gains an
explicit guard against reopening v2 data. Runtime1 repairs were committed first.

## Version and admission

The new manifest format is `recall-gateway-manifest/v2`, with
`accounting_policy = full-context-terminal-v2`,
`bound_version = provider-full-context-v1`,
`rate_version = sol-standard-long-max-2026-10-06`, and an explicit positive
`credential_generation`. The manifest and context also pin
`reasoning_sdk_sha256` and `reasoning_projection_version`. A v1 ledger cannot be opened as v2 or vice versa.
Approval additionally pins independent settlement and context-provenance review
hashes. All previous corpus/configuration/artifact/isolation/approval checks,
fixed upstream, global/role counts, deadline and concurrency limits apply.
The new default output policy is `preserve`: retain the actual positive SDK
`max_output_tokens` up to 128,000, including its current 64,000 default. An output
reduction requires the existing separately reviewed `cap_at_16384` policy.
No count-endpoint calls are introduced. No corpus or retry quota is reduced.

The [model page](https://developers.openai.com/api/docs/models/gpt-6.1-sol)
provides a 1,050,000-token context and 128,000-token output maximum. It specifies
long-context rates of twice input/cache rates and 1.5 times output rates.
Every generation reserves `R = 1_050_000*5000 + effective_output_cap*15000`
integer nano-USD before any connection or send. This prices every input token
at the long-context cache-write maximum and every output token at the
long-context output rate. Context and output maxima are reserved independently,
which is conservative. The [cache guide](https://developers.openai.com/api/docs/guides/prompt-caching)
describes cache writes as an input category rather than an additive charge.
The [Responses reference](https://developers.openai.com/api/reference/python/resources/responses/methods/create)
defines usage totals/details and a combined visible-plus-reasoning output cap.
These are provider-contract assumptions, not measured account billing evidence;
standard nonregional text/function calls only, no separately billed services.
The facts were opened again during source review. Final approval must reverify
rates. Taxes, unrelated charges and provider billing defects are outside this
bound. Embeddings retain the existing fixed nonrefundable reserve.

## Concrete API and files

New `settlement.py` exposes:

```python
POLICY = "full-context-terminal-v2"
RATE_VERSION = "sol-standard-long-max-2026-10-06"
FULL_CONTEXT = 1_050_000

def validate_terminal(body: bytes, *, streaming: bool, model: str,
                      output_cap: int) -> TerminalUsage: ...
# Frozen TerminalUsage: response_id, input_tokens, output_tokens,
# cached_tokens, cache_write_tokens, reasoning_tokens, charge_nano_usd,
# replay_items (canonical SDK reasoning projection bytes).

class SettlementLedger(Ledger):
    def __init__(self, directory, manifest_hash, *, create,
                 context_identity: dict, now=clock): ...
    def reserve(self, request, authorize) -> tuple[int, float]: ...
    def finish(self, identifier, *, outcome, status, response_hash,
               response_bytes, usage, halt): ...
    def settle(self, identifier: int) -> dict: ...
    def authorize_reasoning(self, item: dict, scope: Scope) -> None: ...
    def receipt(self) -> dict: ...
```

`settle` accepts only an existing dispatch ID, never caller usage or a refund
amount. It rereads bounded captured files, validates every hash and transport
receipt against the immutable dispatch/manifest, and runs the strict validator.
`finish` calls settlement only after the transport has fsynced its capture.
The original physical monotonic deadline is bound once before capture/connect;
read completion and the observation after all capture fsyncs are persisted.
`Capture.finish` records original deadline/read completion in its fsynced receipt;
only after it returns does `record_transport` durably record capture completion.
Late fsync or first settlement after the original deadline retains the full
reserve. Exact replay of an already committed settlement never invents a new
deadline. The old advisory `usage_from_response` is ignored as authority. `settle` is
public for an exact idempotent acknowledgement retry; it does not resend.
Independent tests can call it with real temporary capture files and SQLite.
No test-only settlement bypass or permissive caller proof constructor exists.

`wire.validate` gains an explicit context policy and reasoning-authority
callback; defaults retain v1 behavior. `Policy`, `launcher` and `Handler` select
v2 only from its pinned manifest. The existing forwarder gains bounded capture
metadata and strict v2 HTTP-header validation. No alternate network client is
introduced. Capture still retains exact original, forwarded and response bytes
privately; framing/deadline completion, content type and policy/scope identity
are included in the durable receipt used by settlement.

## Durable state and transaction

A v2 accounting-mode marker is persisted in the original creation transaction.
The base v1 constructor rejects that marker or a settlement table; the v2
constructor requires its exact marker. No mode conversion is performed.
The inherited dispatch row preserves original `reserved`, request hashes,
model, scope, output cap, started/finished time and permanent physical attempt.
A new singleton `settlement_policy` records policy/rate/manifest/context identity.
A new `settlement` row keyed by dispatch ID records reservation state,
exact `Scope` identity (including binding_id, connection_id, profile_id and
credential_generation; these are required for v2 and default empty in v1),
`reserved_nano_usd`, immutable binding hash, terminal capture digest, unique
response ID, strict usage, `spent_upper_nano_usd`, and settlement decision digest.
Generation begins `reserved`; embeddings are `nonrefundable`. Successful
terminal generation becomes `settled`. Refusal, incomplete capture, error,
unknown usage and crash remain fully liable. Admission includes all settled
charges plus all unresolved and embedding reserves; it never sums only active
connections. The historical reserve field is never overwritten with usage.

One FULL-synchronous `BEGIN IMMEDIATE` transaction compares the original
request/scope/manifest/rate binding, original reserve and capture digest; inserts
the unique response ID; stores the decision; and changes liability from `R` to
`A = 5000*input_tokens + 15000*output_tokens`. The sum of settled charges only
increases. Only unused `R-A` is released. Original role/global counts and clock
start never change. Repeating the exact committed decision returns it without
mutation; conflicting capture, rate, scope, ID or amount halts and refuses.
A response ID cannot settle two dispatches. Integrity is checked on reopening;
missing policy rows or unsettled interrupted sends halt instead of migrating.

Crash before capture durability or before settlement commit retains `R`.
Crash after commit returns the exact committed decision on retry; no second
release. A capture/database error cannot reset the anchor or authorize a new
budget. A $75 run can stop honestly incomplete before all 432 attempts finish.

## Strict terminal schema

Require complete bounded HTTP framing before the original deadline, 2xx status,
appropriate JSON/SSE content type, and no ambiguous Content-Length/Transfer-
Encoding headers. The validator consumes the entire exact body as strict finite
UTF-8 JSON with duplicate keys refused. JSON must contain one completed Response.
SSE parses every data event, requires clean final event framing and exactly one
`response.completed`, consistent Response IDs, and no failed/incomplete terminal
or later output. Each streamed output_item.done or reasoning.done item must be
byte-equivalent under canonical JSON to that ID's final terminal output item;
conflicting/missing final items refuse settlement and reasoning provenance.
Unknown event accounting semantics retain the full reserve.
The conventional final `[DONE]` marker is allowed once after completion only.

The terminal Response must have the exact approved model identity, `object` of
`response`, nonempty unique ID, `status` of `completed`, the original effective
`max_output_tokens`, `service_tier` of `default`, and null error/incomplete details.
Usage requires exactly `input_tokens`, `output_tokens`, `total_tokens`,
`input_tokens_details` and `output_tokens_details`. All counts are explicit
integers, never booleans, missing defaults, floats or negatives. Input details
require exactly cached and cache-write counts; output details require exactly
reasoning count. Enforce `input+output=total`, cached+written<=input,
reasoning<=output, input<=FULL_CONTEXT, output<=original cap, and `A<=R`.
No cache discount or reasoning subtraction/double-count occurs. Unrecognized
usage categories cannot release money. Strict compatibility failure preserves
liability and is reported; it is not silently reinterpreted.

## Inline reasoning provenance

Allow fully inline reasoning only when a prior strictly completed and captured
response in this same run/account/credential generation/configuration/backend/
space/role/model emitted it. Store hashes in SQLite; the private capture remains
the authoritative full body. At admission and immediately before sending,
compare each reasoning item with that original capture under its exact scope.
Rehash it on every use; do not trust a submitted ID or ciphertext byte length.
External previous-response/conversation/item/file/media references and background,
compaction or hosted tools remain refused. Truncation is explicitly disabled.

The inspected vendored SDK 0.8.51 flattens reasoning summary parts using its
literal newline-join loop, then replays a single summary_text part (or an empty
array for an empty string). The named `sdk-0.8.51-reasoning-projection-v1` therefore
binds the exact original ID and encrypted_content plus that deterministic summary
projection. It never rewrites the forwarded request. Preserve raw original
terminal items alongside the projection hash so reviewers can compare both.
Undocumented reasoning.phase remains refused; assistant message.phase remains
supported. D reviewed this exact projection in the vendored client source; its pinned SHA
is `105229842f1a221277513653d5396c8e36a116384dde406182f2e173cdd0f8c3`.
The version/hash enter every capture binding and are checked on admission and
before send. Actual SDK replay
remains an unexecuted acceptance requirement.

## Independent acceptance hooks

D can test `validate_terminal`, `SettlementLedger.reserve/finish/settle/receipt`,
`authorize_reasoning`, and the ordinary `Forwarder` using sockets-disabled fake
connections plus real temporary capture files and SQLite. Required boundaries
include simultaneous last-budget admission, exact arithmetic, unchanged attempt
quotas, duplicate/conflicting settlement, duplicate response IDs, every malformed
usage/detail case, all SSE suffixes, real truncated HTTP parsing, capture/fsync and
commit faults, reopen liability, foreign/changed reasoning, current revocation,
and v1 compatibility. Source implementation and these tests follow this contract;
none have executed for v2 at this checkpoint.
