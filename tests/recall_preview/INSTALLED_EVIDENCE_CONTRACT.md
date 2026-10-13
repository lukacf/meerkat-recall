# Original installed Linux evidence

This independent source contract covers the original 19 Linux offline cases on
both supported backends. It authorizes no execution and establishes no product
acceptance. M owns the pure implementation, F owns original capture, L consumes
its validated output, and D independently reviews their exact source.
`installed-evidence-contract.json` contains the literal static keys, original
producer source, limits and each case's raw predicate correspondence.

Use the existing `reviewed_static = {review, raw_sha256, raw_bytes}` wrapper.
Verify its original bytes and parsed object. Original static inputs are the five
`review.input_refs` keys in the JSON contract, each an exact
`{sha256, bytes, kind}` reference. They exist before execution. The independent
installed review must approve the exact original plan/cases/corpus/driver,
process capture, collector and installer. Helper keys are under the existing
`review.helper_sha256`; there is no separate `reviewed_static[installed]`
authority. Original allocation expiry, backend and exact artifact identities
remain mandatory. No future receipt, PID, capture or generated config hash is
allowed in this static record.

F's `reviewed_plan` points to original `recall-installed-reviewed-inputs/v2`
bytes with exactly `format`, `plan`, `review`, `case_file`, `corpus`. Its `plan`
is the complete original validated plan, compared against the original static
plan object. The other three are original `{path, ref}` captures, matched to
static input references. Do not require the future carrier's hash in a static
review. Original paths are capture inventory keys, never permission for the
pure validator to read a filesystem.

The outer `recall-installed-offline-evidence/v2` fields are listed in the JSON
contract. Its reference values point to original bytes: F's artifact provenance,
observation, capture inventory, process journal, controller and terminal cleanup.
`command_inventory` binds the original `recall-installed-process-journal/v2`.
F's separate `recall-installed-process-evidence/v2` is a producer sidecar;
assembling references from it must not rewrite original records or assert their
predicates. Captured `{path, ref}` values are explicitly unwrapped and matched
to the inventory before use. The observation's `cases` and `subset_observation`
are not independent verdicts.

`validate_installed(reviewed_static, installed_carrier, raw_evidence)` is pure:
it returns exactly `{identity, required_cases}` after every predicate succeeds,
or raises a safe static `ValueError` beginning `dynamic_installed_`. Missing,
unknown, malformed, failed or incomplete evidence never returns a partial pass.
It performs no filesystem, process, network, clock or producer-helper operations.
Original JSON parsing rejects duplicate keys, nonfinite numbers, invalid bounds
and booleans used as integers. Every reference checks kind, original size and
SHA-256 before decoding. Decode original HTTP base64 strictly, hash the decoded
body, validate original completion and protocol framing, parse exact JSON-RPC
IDs and the single typed MCP text result. A summary body does not replace bytes.

Preserve original allocation and process clocks. All case observations and
requests must fall within the original controller/phase deadline, not a new
clock interval. All process intents precede launch; actual unreaped PID,
PPID/group/session/start ticks and stat/executable/stat observations establish
identity without reading secret environment values. Original argv and explicit
environment names must match the reviewed recipe. CLI commands exit zero;
services terminate through original owned graceful cleanup with zero exit,
actual reaping and complete bounded stdout/stderr. Missing identity observations,
forced kill, nonzero exit, drain error, unowned process, inconsistent process
counts or surviving process prevent acceptance. Zero-case or name-only evidence
cannot pass. Intentional absent-service refusal is narrowly scoped to its own
first request.

Keep all original limits: 300 seconds, 128 HTTP calls, five seconds per ordinary
request, 120 seconds for snapshot calls within the same whole deadline, 262144
response bytes, 16 processes, 1 MiB per process log and 64 MiB aggregate capture.
F's 2 MiB metadata/512-event subcaps are charged within the original 64 MiB;
they do not expand it. Count every original attempted request/process, including
failed startup probes. Do not confuse current semantic bootstrap's larger
capture budgets with this offline lane.

The source contract corrects a concrete identity mismatch in the proposed
compatibility seam: the original installed allocation and observation contain
source commit, archive SHA, executable SHA and backend. They do not contain a
runtime input digest, paired package/executable or current run nonce. The
installed component must never invent those fields. Its four-field identity
binds exactly to the current candidate; the current semantic runtime separately
proves every additional original reviewed runtime, paired package, process and
controller identity. This does not claim the offline lane ran the paired server.
Root explicitly accepted this scope distinction at 03:11 UTC. Any claim that
this offline evidence itself proves paired-server execution remains UNMET. The intentionally unapproved Metadata configurations stay unchanged and
are checked against their own original recipe, not current approved providers.

The original case inventory order is declarative. Actual execution issues fresh
CLI credentials before readiness; `pending_is_not_complete` consumes original
requests captured under the preceding acceptance case. Require exact unique
membership and the real dependency chronology, not fabricated extra requests
or marker-order equality. All full baseline head/operation/config comparisons
must use prior original captures, never reconstructed records or case flags.

The existing unqualified gates remain explicit: rendered publication, provider
relevance and dispatch, physical erasure/backup retention/replay, restore read
admission, SIGKILL durability, remote patched server, container PV, Darwin,
consumer adoption and measured load. No result from this helper closes them.
