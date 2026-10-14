# Qualification gateway source — not live-ready

This is a qualification-only Python standard-library gateway and isolated launcher.
It is not a Recall product endpoint. The checked-in manifest is deliberately
unauthorized. No real credential read, paid request, isolation launch or provider
acceptance is implied by its offline tests.

The reviewed proposal remains owned by independent qualification: commit
`1e5f28381f401b39cb1a5e488bf3e7536017b8c8`, `tests/recall_preview/primary-provider-proposal.json`.
Do not edit that proposal, corpus, expected answers or thresholds from this lane.
This directory is a concrete enforcement candidate for independent review.

## What the active path enforces

`launcher.py` validates a private manifest and spending approval, pinned installed
artifact/runtime/launch configuration/gateway source hashes, then opens a durable
run ledger. Only after those checks does it read an explicitly inherited credential
FD. It never looks up an environment key or credential store. The host gateway
listens on a private Unix socket; the child receives newly generated gateway
capabilities only. The approved run directory is fixed in the manifest. A sibling
exclusive anchor prevents accidentally replacing a deleted ledger with fresh caps.
Trusted operators must preserve both ledger and anchor; deleting an anchor is not
an authorized recovery operation.

`server.py` accepts exact HTTP POST paths `/v1/responses` and `/v1/embeddings` with
bounded unambiguous JSON framing and a server-created capability mapped to an
approved backend, named space, operation and model. Caller role headers are not
trusted. Each binding requires its own connection and profile identity. The
installed Recall configuration must independently be shown to use that mapping.

`wire.py` inspects the actual serialized request, including schemas, tool outputs,
instructions and complete plaintext history. It accepts `gpt-6.1-sol`, explicit
medium reasoning, output cap at most 16,384, text-only local functions and approved
small/large embedding models/dimensions. It refuses unknown fields, remote schema
references, hosted tools, media, files, conversation state, `previous_response_id`,
assistant state IDs and all reasoning replay. Requesting encrypted reasoning in
the *output* is preserved; trying to replay it is refused. No reasoning/tool/model
output is silently stripped or substituted. This means the current SDK's opaque
continuations cannot complete through this initial subset.

The default output-cap mode is `reject`: a 64,000-token request is refused before
dispatch. A separately reviewed qualification-only mode `cap_at_16384` may be
selected explicitly in the pinned manifest. It requires an additional approval
reference `output_transform_review_sha256`; no existing strict-mode approval can
activate it. Valid positive integer requested limits up to 128,000 are preserved
in the original body/receipt, while the forwarded limit is at most 16,384. A named
`approved_output_cap:64000->16384` transform records an actual change; smaller caps
remain untouched. Cost reservations still cover the full approved 16,384 output
allowance including reasoning. Product defaults are unchanged. This proposed mode
qualifies that exact transformed gateway configuration, not unchanged direct
production dispatch. Provider truncation/incomplete status remains incomplete and
cannot relax any frozen semantic oracle. The example manifest keeps `reject`.

Outgoing generation JSON explicitly sets `service_tier: "default"` and
`store: false` when absent. The original and forwarded body hashes and exact
transform names are recorded. The official [Responses create reference](https://developers.openai.com/api/reference/python/resources/responses/methods/create)
defines default as standard tier, distinguishes automatic tier selection, and
defines the output cap to include reasoning and visible output. The physical
gateway URL and fixed upstream URL are separate manifest fields: vector endpoint
provenance changes and must not be represented as direct-endpoint parity.

`ledger.py` uses SQLite `synchronous=FULL` transactions and a lifetime process lock.
Every physical attempt reserves its entire worst-case allowance before connection
construction or sending any provider bytes. Retries are new requests with new
reservations. Successful usage, provider errors, cancellation and unknown outcomes
never refund money or counts. In-flight entries found on resume halt the run.
Missing/corrupt storage and capture I/O failures fail closed. Approval and immutable
configuration are rechecked at reservation and immediately before sending after
TLS connection; changing/removing the approval revokes subsequent forwarding.
Requests already sent cannot be recalled by revocation.

| Constraint | Fixed candidate limit |
| --- | ---: |
| Both backends, physical attempts | 432 |
| Money, integer nano-USD | 75,000,000,000 ($75) |
| Original run time | 5,400 seconds |
| Concurrent upstream dispatches | 2 |
| Each dispatch deadline | 120 seconds, bounded by remaining run time |
| Generation input candidate bound / output tokens | 32,768 / 16,384 |
| Embedding input, whole batch | 8,192 |
| Incoming JSON bytes | 131,072 |
| Upstream body and private capture bytes | 8,388,608 |

Each backend additionally has candidate authenticated quotas: **combined document
and query embeddings 76**, triage 0, extraction 56, support verification 56, summary 8,
combined promotion/identity 4, truth maintenance 4, combined wiki stages 12. Spaces
share these backend quotas. Every generation dispatch reserves 245,760,000 nano-USD;
every embedding dispatch reserves 1,064,960. Thus 280 generation + 152 embedding
dispatches reserve $68.97467392, below $75. The combined embedding allowance is a
root-approved design direction pending D's numerical/configuration proposal review;
separate document-28/query-48 limits are **not enforced** by this gateway. The
calculation uses the proposal's
highest input/cache-write rate and large embedding rate, without discounts.

Wall time and monotonic time share the original durable start. A rollback is
rejected; a new host boot cannot resume. Restart never extends the deadline. A
transport watchdog closes the socket at its deadline, including slow headers.
EOF produced by that watchdog is incomplete even for close-delimited responses;
a terminal SSE event also cannot override a short HTTP Content-Length.
There is no upstream retry, redirect following, ambient proxy or runtime DNS;
TLS certificate/SNI checks always name `api.openai.com` at port 443, using an
explicitly approved global numeric address.

`capture.py` retains bounded original and forwarded synthetic JSON bodies and
byte-identical upstream JSON/SSE/error-body prefixes in private mode-0600 files.
Authorization headers, capabilities and real keys are excluded. Operational logs
contain no prompt/body/header/exception text. Capture is fsynced before the ledger
marks completion. Streaming may expose bytes before the final capture sync; only
the completed durable receipt establishes complete capture. A crash leaves a
nonrefundable incomplete dispatch and cannot be reported as completed acceptance.
Receipts are sanitized hashes/counts/status/usage, never a semantic quality verdict.

The 8 MiB transport/capture ceiling is an **explicit new qualification proposal
delta**, pending D review; it is separate from the 262,144-byte public Recall
search-response cap. Oversized transport produces incomplete qualification, never
clipping a successful answer or silently raising the limit. At most 432 captures
bound body storage to approximately 3.4 GiB plus bounded requests and metadata.

## Isolation and unresolved prerequisites

`bridge.py` runs only inside bubblewrap with `--unshare-all`, a new session,
`--die-with-parent`, clean environment, closed inherited descriptors, private
proc/home/tmp/dev and an explicitly hashed read-only runtime tree. Only the private
worker artifact directory and single gateway socket are mounted from outside.
Runtime packaging must materialize regular files/directories; symlinks, sockets,
devices and writable/set-id runtime entries are refused. The bridge checks that
its network namespace differs from the parent and has only loopback with no IPv4
route. It bridges loopback port 43129 to the sole mounted gateway Unix socket.
The gateway performs fixed upstream dispatch, not arbitrary tunneling. Missing
isolation never falls back to ordinary host execution. Child output is suppressed;
the qualification harness must write its public source/search captures to `/work`.
`SKIP_AUTH` and imported JWT secrets are forbidden. After isolation checks, the
bridge generates a fresh synthetic HS256 signing key for the harness and its
Recall child processes; it never reads a host signing key. Optional approved
issuer/audience strings come from the hashed launch specification. The harness
must sign real API principals and exercise actual authentication/authorization.
This synthetic local auth key is separate from provider credentials and is not
written to the gateway's captures, receipt or logs.

The source remains **unready for live use** until all these are reviewed:

1. The token bound is a candidate assumption, not a demonstrated provider-internal
   proof: whole canonical UTF-8 body bytes plus 4,096 protocol tokens, 64 per input
   item and 64 per local tool; embedding bytes plus 64 per batch item. Independent
   review must establish exact tokenizer/framing sufficiency or replace the bound.
   Usage exceeding it halts the run and records the violation, rather than
   retrospectively claiming the hard-dollar proof held.
2. Actual per-operation connection/profile/secret resolver configuration is pending
   B's example and independent review. Recall requires document and query embeddings
   to share the active profile. The `embedding` capability therefore authenticates
   that operation pair together with a 76-per-backend quota; it does not trust a
   caller's document/query label. Separate same-model generation profiles/connections
   authenticate generation roles. The gateway refuses different capability bindings
   sharing a connection/profile identity, preserving one embedding binding per
   backend/space. Separate embedding operation counts require independent worker
   receipts and are not enforced quota claims.
3. Opaque SDK continuation is deliberately unsupported; stateless full plaintext
   replay requires an actual compatible call path, not response rewriting here.
4. The concrete minimal runtime package, credential custody, binding configuration,
   egress namespace proof and public source/search captures need installed testing.
   Network isolation assumes a trusted host/kernel/operator and this fixed launcher.
5. D must review raw synthetic capture custody, byte ceiling and independent
   negative cases. Root then obtains exact-run user spending approval separately.

The private approval schema is enforced in `Policy.check`: exact manifest hash,
`authorized: true`, expiry, user approval reference and nonzero independent review
hashes for bound, isolation, bindings and capture scope. These are trusted host
approval references, not signatures or proof generated by the gateway. The worker
cannot mount or mutate that approval. Source approval alone never authorizes spend.

## Offline checks

After the full source/test/documentation checkpoint, run only:

```sh
python3 -B -m unittest discover -s tests/recall_preview_semantics/gateway -p 'test_gateway.py' -v
git diff --check
```

Tests use in-process fake HTTP connections and private temporary files, without
listening sockets, real secrets, provider traffic, downloads or bubblewrap launches.
They cover full-body validation, Unicode and batched-input accounting, refused
state/media/tools/models/tier, last-dollar races, shared-space operation caps,
nonrefundable unknown/error usage, crash/resume/clock/missing-ledger denial,
revocation boundaries, exact fake SSE/error captures and capture/transport failure.
Passing these tests does not prove live provider/token/isolation/model semantics.

## Separate full-context settlement policy

`manifest.settlement.example.json` is an inactive v2 example. It preserves the
actual SDK output cap and reserves the full documented model context before
sending, including fully inline encrypted reasoning whose provenance was captured
in this same run and exact authorized scope. It introduces no count-endpoint calls.
The original v1 manifest and nonrefundable accounting remain unchanged; its
constructor now rejects a v2 ledger explicitly.

`TERMINAL_SETTLEMENT_CONTRACT.md` states the reviewed candidate contract and
provider-billing assumptions. `terminal_usage.py` validates complete JSON/SSE
terminals; `settlement.py` applies a once-only durable conservative charge from
exact fsynced private captures. Unknown usage, framing or identity retains the
full reserve. A $75 cap may stop the run before all 432 attempts complete.
Neither mode is a claim of model quality, installed isolation or paid approval.

The new offline suite uses fake upstreams, real temporary SQLite/capture files,
and forbids socket connections. Run it together with the retained v1 suite:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s tests/recall_preview_semantics/gateway -p 'test_*.py' -v
```

At source checkpoint, v2 still requires independent D execution/review and actual
SDK/isolation acceptance; no live manifest or credential is supplied here.

D source review added an explicit persisted v2 mode marker, exact streamed-item
versus terminal-item consistency, and original deadline/read/post-fsync capture
timing. A capture that finishes late cannot release liability. The vendored SDK
source hash and reasoning-projection version are mandatory v2 context identities.

The separate D28 embedding workload proposal uses the explicit inactive
[cohort manifest](manifest.cohort.example.json) and
[cohort accounting contract](COHORT_CONTRACT.md). It has its own SQLite mode,
original allocation and phase gates; it never expands the 432-dispatch v1/v2
budgets. All execution and independent qualification prerequisites still apply.

Packaged extraction skills are explicit runtime inputs. A launch spec may set
`PIPELINE_SKILLS_DIR` only to a canonical existing directory beneath the immutable
`/opt/recall/` mount. The reviewed runtime tree includes the complete skill files;
no checkout-relative or host directory is substituted. The optional setting is
part of the exact launch configuration hash and grants no model dispatch budget.

Original accounting export is an explicit source-only v2 extension. The legacy
v1 anchor, launch context, cohort begin, and cohort close schemas remain strict
and unchanged. `close_with_evidence` returns a small
`recall-exclusive-cohort-close-envelope/v2` containing the exact original close
and an export-index reference (or no export for incomplete closure). It uses the
same original close intent; it creates no new phase, provider send, reservation,
refund, approval or public request. The existing 65,536-byte control reply bound
still applies.

The parent creates `/gateway-accounting` as a read-only bind into the original
child. A separate `recall-gateway-accounting-channel/v1` sidecar binds the exact
original anchor hash, directory device/inode/owner/mode and reader source hash.
The child checks actual `/proc/self/mountinfo` per-mount read-only flags and the
same directory identity before reading. This channel exposes neither SQLite nor
keys, capabilities, credential or unrelated headers, or ambient paths. Only final synthetic capture files
from an already durably closed cohort are hardlinked; each original hash, owned
regular-file identity, complete capture and exact link relationship are checked.
Metadata files are written exclusively and fsynced; index publication comes last.

`cohort_bridge.provider_accounting_with_evidence(...)` is the explicit v2 control
adapter. `export_cohort_evidence(manifest, begin, close, export_reference)` opens
that same original index without another control or provider call. Its result
contains `reference`, `raw_evidence`, `files`, and separate raw `channel`
observations (sidecar, actual mountinfo, original index and directory). The typed bundle is
`recall-dynamic-provider-accounting/v2` with exactly `format`, `begin`, `close`,
`baseline`, `phase_clock`, `sealed`, `capture_inventory`, and `dispatch_records`.
The baseline is the exact original stored SQLite text, validated against the
original begin/close hash; it is never regenerated from a current receipt.
The sealed original `after` ledger is retained even if later cohorts run.
Dispatch associations retain the original ledger row, binding and send-intent.

Kinds are `provider-accounting`, `provider-cohort-begin`,
`provider-cohort-close`, `provider-baseline`, `provider-phase-clock`,
`provider-sealed`, `provider-capture-inventory`, and `provider-dispatch-records`.
Both original and forwarded request files use `provider-request`, since their
exact bytes may be identical. Original paths and intent hashes preserve their
separate roles. The other capture kinds are `provider-dispatch-intent`,
`provider-response`, and `provider-capture`; the transport index has kind
`provider-accounting-index`.

The phase clock exposes the original run and phase starts, original monotonic
deadline, fixed phase duration and closed state. `closure_timestamp` is explicitly
null because the existing ledger did not retain such a timestamp. Independent
validation must use the original dispatch intervals and actual controller end;
it must not substitute an accessor's current time as the original close time.

The caller-owned `Mapping` reads immutable records lazily and independently
checks every loaded length/hash. It retains at most an 8-MiB default cache,
permits no cache above 256 MiB, bounds each original record at 8 MiB, and limits
original indexed disk bytes to the original 20-GiB capture cap with a finite lookup
count. Validators must not materialize `dict(raw_evidence)` or retain all body
bytes. The 20-GiB disk ceiling is not a memory allocation. These source controls
do not establish actual namespace isolation, current authorization, paid-provider
qualification, or permission to run the launcher.

Cohort captures now also retain `capture.json.transport` with format
`recall-provider-transport/v2`. The original physical `Forwarder` observes
`started_mono` after reservation, `request_started_mono` immediately
before the send, the original `deadline_mono`, and
`read_completed_mono` only after actual EOF and the existing deadline/framing
checks. The deadline is clamped to that dispatch's original cohort deadline;
it is never extended. Capture/fsync or SQLite completion wall times are not
substituted for response-read completion. Partial failures retain observed facts
with `complete=false` and no invented read-completion timestamp.

`response_headers` contains exactly the allowlisted Content-Length,
Transfer-Encoding, Content-Encoding and Content-Type value lists, preserving
duplicates for independent refusal. The flat `eof_observed`, `remaining_length`,
`chunked` and `response_closed` fields retain actual HTTP parser observations.
EOF at or after the deadline is never marked observed in a successful proof.
No other response headers are retained. Missing actual header/parser evidence
refuses the dispatch and leaves an incomplete capture; body bytes alone cannot
prove framing. The source subcaps remain four values per list, 1,024 UTF-8 bytes
per value and 4,096 bytes across the encoded header facts. Tests supply explicit
mock HTTP metadata or use the real HTTP parser; the producer never fabricates
missing facts.
New source controls use the real standard-library HTTP parser over in-memory
wire bytes to distinguish complete Content-Length/chunked bodies, truncation,
and read completion preceding a later capture finish. Existing settlement
transport records keep their own original schema and accounting rules.

Digest membership and key iteration on the lazy mapping inspect only its bounded
index. They do not load bodies or spend the original disk-read budget; only an
explicit byte lookup does so. This prevents an inventory-validation membership
check from becoming an unintended eager read of all provider captures.

The `directory` observation is captured from the original `lstat` during the
same `observe_channel` call that checks the sidecar. Its exact JSON fields are
`format: "recall-provider-accounting-directory/v2"`, `path`, `device`, `inode`,
`uid` and `mode`; the raw bytes are bounded at 16 KiB. These are observed facts,
not values later reconstructed from the sidecar. The lazy resolver also refuses
a directory identity change between that original observation and opening the
index. The runtime validator owns the independent channel-custody predicate and
receives all four original channel observations in its separately versioned
typed carrier. The provider-accounting bundle retains its original eight fields.

Repeated local reads have a separate finite budget derived from the active
consumer: S validates the original preparation at `prepared` and `premeasurement`;
each boundary runs one L inventory verification and one F provider verification.
The resolver therefore permits four indexed-byte passes, plus the exact extra
G/S/L metadata reads: bundle four times, begin five, close three and capture
inventory twice. Each of those metadata kinds must occur once in the index;
begin/close retain 64-KiB limits and bundle/inventory retain 8-MiB limits. The
maximum repeated-read budget is 80 GiB plus 48.5 MiB, while indexed original
capture remains at most 20 GiB and the cache at most 256 MiB. Every uncached
read still checks original directory/file identity, length and digest, and the
original finite access-count limit remains. This grants no additional capture,
network, provider, storage or execution allocation. An uncached offline control
performs the exact two-boundary sequence and requires the next read to refuse.
