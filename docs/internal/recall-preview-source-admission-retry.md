# Source acceptance under same-space contention

Public source PUT reaches `SourcePublicationRepository::accept`. Its native
transaction intentionally writes the shared space/configuration fence before
accepting the source, immutable revision, desired head, authorization envelope
and operation. Independent simultaneous sources can therefore conflict despite
having different external identities. Removing that fence would change the
space/configuration safety contract.

The source136 HomeCore run submitted fourteen distinct sources concurrently.
Ten calls reported the storage transaction's retryable conflict; four were
accepted and later completed. Existing generic checked queries already retried
this error, using four total attempts and fixed 25/50/100 ms delays. The recorded
success times and failure wave are consistent with that finite contention
budget. There was no per-request SQL attempt telemetry, so the original evidence
does not prove each rejected request's attempt count. The harness's 0.001–0.004
second values were dispatch offsets, not rejection latency. Failed durations,
HTTP statuses and headers were not captured.

Source acceptance now has a separate private executor for its single explicit
BEGIN/COMMIT transaction. It permits at most sixteen total attempts. It admits
no new retry after two seconds, including scheduler delay during backoff. This
is a **new-attempt admission bound**, not a two-second request deadline: an
in-flight query retains the existing database deadline and cancellation/writer
custody. Exhaustion returns the last actual storage error.

Backoff starts at 2 ms and doubles to a 64 ms cap, plus independently sampled
0–31 ms jitter. Retry requires a native `TxRetryable`, its exact native failed
transaction detail, or the exact supported remote query representation. Every
error in a response must be that conflict or a rollback marker, with at least
one definite conflict. A bare rollback marker, arbitrary thrown text, mixed
semantic failure, timeout, transport error or unknown commit acknowledgement
does not authorize replay. Existing generic query and provider policies are
unchanged.

The native binding value is captured once and reused with the same SQL. The
original source content, observed time, request/input fingerprints, operation
identity, accepted timestamp, source generation and all authorization/CAS/
configuration/document/attempt/lifecycle predicates remain intact. The executor
does not reprepare authority, regenerate a receipt, enqueue work or call a
provider. Each transaction independently applies its existing physical fences;
source-head nonce-only changes remain distinct from semantic epoch changes.
Existing identical-intent receipt reconciliation still runs after an error.

The focused tests barrier-synchronize fourteen repository acceptance calls on
each backend, then check exact immutable inputs and envelopes, receipt replay,
observed-time preservation, changed-content/intent refusal, simultaneous
identical acceptance, and one-winner generation replacement without loser rows.
They check semantic invalidation and the native source-head nonce distinction.
These are active repository admission tests, not HTTP authentication/status
coverage. The same pure production classifier/controller source is also
compiled into the pipeline test binary to exercise definite-abort, mixed-error,
success, exhaustion, admission expiry, scheduler oversleep and last-error
behavior. No provider or worker is involved in these controls. The retained
Graph/finalizer tests and a new consumer run remain separate evidence scopes.
