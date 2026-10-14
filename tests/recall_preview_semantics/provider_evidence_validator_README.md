F's `validate_provider_accounting` is the pure provider predicate called by L's
prepared-fixture composition. It accepts the original accounting reference,
immutable raw Mapping, reviewed identity, original controller wrapper, current
named-space configurations, original preparation observation and capture bounds.
It returns exactly `begin`, `close`, and `phase_clock`. It reads no paths, clocks,
processes, network or producer helpers. It does not authorize execution.

Every original baseline, sealed receipt, phase clock, inventory and dispatch is
hashed and cross-bound to the exact controller/allocation and original public
observation. The old ledger prefix remains intact. Original cohort ordering fixes embedded
preparation at 1 with an empty initial ledger and native preparation at 3 with
the retained prior embedded cohorts; prior measurements are not requalified by
this predicate. Preparation is limited to
450 physical reservations, 225 per each of the two named spaces, concurrency 2
and the original 3600-second phase inside 7800 seconds, 2100 calls and $3 overall.
Unknown usage and upstream errors retain the full original 1064960-nano-dollar
reservation. They never refund budget. Any failed or non-2xx dispatch, including
one in the retained baseline, refuses qualification even if its cost ledger was
settled successfully. Successful embedding replies require original JSON objects
and a JSON content type; an unexpected event stream never qualifies through
HTTP EOF or a producer completion flag. A zero-duration wall interval is counted
conservatively with starts before ends; ambiguous timestamps cannot prove a
third simultaneous reservation absent.

Each actual request is bound to the current document-embedding profile,
connection, credential generation, account and original gateway capability via
its configured credential reference. The profile must also be the configured
active embedding profile in that exact named space. The SDK request limit is 64 inputs and
8192 serialized upper-bound tokens. This differs from the separately owned
controlled-spy full-workload fixture. Both original and forwarded request files
use the same provider-request kind because unchanged bytes legitimately share a
hash. Other cross-kind aliases are refused.

D's provider transport contract requires original finite monotonic send/read
samples, original deadline at most 120 seconds, and literal response header and
HTTP parser EOF facts. Missing old facts stay insufficient. Content-Length,
chunked and close-delimited bodies have distinct predicates; duplicate/conflicting
framing, compressed content, early EOF, watchdog EOF, incomplete observations,
reconstructed completion, changed raw bytes and advisory usage mismatches fail.
SQLite finish time cannot replace the physical read-completed observation.

Raw references are read lazily from the caller's Mapping, with at most 16384
references, 20GiB original provider inventory and an 8MiB per-record maximum.
The validator retains digest metadata rather than raw-body caches. Caller-owned
cache policy remains bounded separately; no inventory is eagerly materialized.
Original responses prove accounting and framing only, not embedding quality.
Public-request-to-physical-dispatch attribution remains explicitly unmet.

The F-owned test source builds in-memory synthetic literal transcripts for both
backends and altered-evidence controls. It blocks process, socket and clock use
while evaluating the predicate. These are pure predicate controls, not product,
provider, gateway or installed-runtime acceptance, and they execute no other
owner's helpers or test suites.
