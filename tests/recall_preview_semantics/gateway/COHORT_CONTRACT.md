# Separate D28 cohort accounting

This source adds an explicit `recall-exclusive-embedding-manifest/v1` launcher
mode. The checked-in example is inactive. It does not change the original v1
432-dispatch/$75 ledger or the v2 terminal-settlement proposal. No provider call,
credential read, installed service, isolation launch or load result is implied.

The mode pins D's unchanged allocation proposal at SHA-256
`0a54fcd4c6359e39611567a06857f2d61975b3f19a80052f6ea49691f6d1633f`, including
its exact corpus and contract hashes. Activation also requires D's separately
frozen typed Source+Doc preparation contract (`b1fd4c43e1c8c506ca13e261a29d65a7d06246bf11ef53c02e97945cd3ef56d1`)
and representation delta (`51b34aec951d783a93b903a1468fba9a8833ba2f31f705bd784c66c9b26688f8`).
Both preparation and measurement caller manifests carry mandatory
`preparation_contract_path`/`preparation_contract_sha256` and
`typed_delta_path`/`typed_delta_sha256`. The gateway revalidates these original
files at each admission/send boundary and includes both hashes in capture
identity. A normalized default-level-zero source cannot substitute for the
frozen household-policy level 3/business-policy level 7 preparation.
The proposal remains marked inactive;
execution requires a separate current private manifest and explicit spending
approval with independent bound, isolation, binding, capture, cohort and exclusive
run reviews. The actual binary, entire packaged runtime, gateway Python sources,
launch configuration, account, credential generation and four space/profile
bindings are fixed. Original scope names and profiles are retained.

Admission uses four single-use cohorts in order: elephant-db preparation,
elephant-db measurement, surreal-db preparation, surreal-db measurement.
Preparation permits 450 physical attempts (225/space), at most two active, and
3600 seconds. Measurement permits 600 (300/space), at most 50 active, and 120
seconds. The original allocation stops at 2100 attempts, $3 or 7800 seconds.
Each physical request is bounded by 120 seconds and the remaining original
cohort/allocation interval. Neither failed requests nor recovery restart the
count, interval or money allowance. Ordinary SDK-three/worker-three arithmetic
explains capacity; it is not a guarantee about crash recovery, blocked-attempt
refunds or extra stage work. The installed retry configuration must explicitly
pin `PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS=3`.

Only embedding requests are admitted. The pre-send wire guard checks the actual
1–64 input strings, exact model/dimensions, and a conservative whole-serialized-
JSON byte bound plus 64 bytes per item, at most 8192 total. Input/provider output
caps remain 131072 bytes/8 MiB. Every admitted attempt reserves 1064960 nano-USD
permanently; the maximum is $2.236416 under the reviewed rate assumption. Unknown
usage, errors and incomplete captures never refund. Reverify official provider
terms before any live authorization.

Gateway capture storage is finite: 2100 * (2*131072 + 8388608 + 32768) =
18235392000 bytes, below the separate 20 GiB capture allowance. The remaining
space covers bounded SQLite and four control receipts. Startup checks available
capacity plus a 1 GiB margin. This is a bound on gateway capture/ledger payloads,
not a filesystem quota on the separate worker's database and artifacts; packaged
preparation/collector bounds and host capacity still need independent review.

`cohort_bridge.py` provides:

- `authorize_provider_allocation(manifest, contract, allocation)`: verify the
  supplied contract/allocation against their original files and send the exact
  pinned identity to the isolated supervisor. A FULL-synchronous baseline and
  original monotonic deadline precede admission.
- `provider_accounting(manifest, observation, allocation)`: close admission
  durably, retain the original cohort identity, require all reservations to drain,
  and re-read exact original/forwarded/response capture files before sealing the
  ledger delta and capture inventory. An outstanding reservation returns `unmet`
  and keeps admission closed. The same exact close can be replayed; a changed
  observation cannot replace its original seal.

Control uses a separately minted capability over the same private loopback/Unix
bridge. SDK capabilities cannot begin or close a cohort. The host provider key
never enters the sandbox. New mode alone expands the bridge/server capacity to
64 connections, allowing the explicit 50 physical request ceiling plus bounded
control traffic. Original modes retain their limits. Before each physical send,
a separate durable send intent identifies the original reservation ID; duplicated
request bodies are never merged or matched to a public query by time or hash.

The return value establishes accounting for the exclusive gateway cohort only.
`per_request_association` remains `unmet`. It does not prove that a public request
caused a specific SDK attempt, that prepared node/index inventory is complete, or
that source/search/model quality passed. Isolated caller/config exclusivity is an
explicit reviewed launch condition, not a claim against a compromised host or
unrelated outside account activity. S owns public preparation and measurement;
D independently judges their captured evidence. Public surfaces that cannot prove
an original private manifest leave that preparation prerequisite unmet. The
remaining approximately 140-search stress, large/shared payload, revision/no-op,
permission/retraction/outage, fallback and Graph semantic packets remain separate
required qualification work.

Offline tests use real temporary SQLite and fsynced capture files with synthetic
transport responses. Their constructor fixtures are not real approvals and
cannot pass launcher artifact/isolation checks. No source-only test result is
live acceptance.

The public bridge accepts exactly one bounded Content-Length JSON control reply,
rejects Transfer-Encoding and duplicate framing headers, requires the complete
body, and rejects duplicate keys, nonfinite numbers and invalid UTF-8. It closes
its connection on every outcome and does not retry control calls. The nested
socket timeout is ten seconds; callers must retain the preparation/measurement
adapters' absolute begin (ten seconds) and close (130 seconds) deadlines, including
around a synchronous bridge invocation. Offline fake-transport tests exercise the
parser and closure behavior, not actual network scheduling or isolation.
