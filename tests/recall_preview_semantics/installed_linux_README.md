# Installed Linux offline qualification source

This driver is source preparation, not a product qualification receipt. No
candidate has been installed or launched by this lane. D owns the expectations
and final verdict; integration owns later authorized execution. The draft cases
were specified before any candidate observations. They do not change D's corpus,
semantic expectations, thresholds, or collector.

`scripts/qualification/installed_linux_offline.py` uses the real release
verifier/installer and an exact installed executable. It reuses D's frozen
`tests/recall_preview/fixtures.py` input generator and strict JSON/deadline code.
It never builds or starts a repository binary. Its source is independent from
the gateway/live-provider driver.

## Covered sequence and limits

Each separately allocated backend run uses a fresh private directory, signing
key issued by the installed CLI, global operator, ordinary two-space service,
and one-space service. The global operator carries only the explicit management
scopes accepted by `GLOBAL_ADMIN_SCOPES`, with no data clearance or selected
space. Named service tokens retain explicit data scopes and clearances; current
space grants authorize their source/doc operations. Each classified space also
gets its own explicit `--bootstrap-space NAME` token with only the four bootstrap
management scopes and that space's frozen policy/level clearance. These 900-second
tokens create the spaces and manage their current grants; the global operator
cannot acquire classified record clearance. Source reads and writes still use
ordinary service tokens and current grants. The two extra CLI issue commands fit
the unchanged 16-process bound. The child environment is
constructed explicitly. No
ambient JWT, provider credentials, skip-auth, config, or proxies are inherited.
A fresh Linux network namespace containing only loopback is mandatory; namespace
checks fail closed before loopback is enabled. Kernel observations are custody
evidence, not protection against a compromised host or dishonest operator.

The fixed `installed_linux_cases.json` sequence covers:

1. Exact trusted archive, source commit, manifest, installed binary digest and
   version; absent loopback service; live but unconfigured bootstrap readiness.
2. Actual runtime initialization, explicit migration for native SurrealDB,
   disabled workers, distinct named spaces, policy levels, dimensions, purpose,
   and skill. Processing configurations have no connections or approvals.
   Independent authenticated GETs verify the actual stored settings and record
   identity, both initially and after restore/restart. The storage-owned index
   epoch and updated timestamp may change during source-head writes (schema
   033/038); admin generation, record ID and all other configuration stay fixed.
3. Signed real HTTP/MCP calls, unauthenticated denial, same-token single-space
   positive and wrong-space negative, path/header mismatch, exact current-grant
   revocation and restoration using generation preconditions. Negative responses
   must have the exact typed nonretryable error shape, contain no data payload,
   and exclude original source-text and credential canaries. HTTP and MCP
   wrong-space checks repeat after actual source acceptance.
4. Frozen original source text accepted as pending, idempotent receipt identity,
   operation status, and identical HTTP/MCP pending coverage. Original query text
   is reused only to check pending coverage, with an explicit baseline revision
   constraint; this is **not a retrieval relevance observation**.
5. Typed input bound, registered named export, replacement source acceptance,
   actual restore, and original full head **and operation** equality. The durable
   recovery directory is outside the restored store and retained across restart.
6. Graceful shutdown/reopen of the same executable and storage, exact retained
   identities, then read-only tool inventory, typed write rejection, and read.

Each run has a 300-second work budget, 128 HTTP attempts including startup
failures, 5 seconds per ordinary HTTP request, 120 seconds per snapshot request,
262144 response bytes, 16 owned child invocations, 1 MiB per child log stream,
and 64 MiB of HTTP capture files. Startup polls at most once per second within
20 seconds per launch. Native Surreal uses one additional service launch and
explicit migrate command. A snapshot timeout fails the case; it is not evidence
that the server canceled the operation. Cleanup can add a bounded final child
reap interval after the work deadline. These are driver safety bounds, not
promised product latency or load acceptance thresholds.

Logs drain through bounded private pipes. On overflow the driver terminates its
owned process group, retains the prefix, and fails the run. Leader exit is
observed with Linux `waitid(WNOWAIT)`, preserving its reserved PID until owned
process-group cleanup and pipe closure. Cleanup can still terminate descendants
after leader exit; signals and final reap share a lock to prevent PID-reuse
signals. The driver retains its child handle until reap succeeds. No file-size rlimit
is placed on the product's database or backup files. Disk capacity/quotas for the
trusted archive, installed tree, database and backups remain an execution-host
prerequisite; the HTTP/log capture caps are not a whole-run disk quota. No files,
cache, process, or namespace owned by another run are deleted or signaled.

Private raw response receipts include original body bytes, framing completeness,
hash, exact request, selected space, role name, and duration. Bearer values and
signing-key contents are excluded. Invalid JSON, duplicate keys, nonfinite
numbers, incomplete framing, oversized bodies, ambiguous identity, and incomplete
SSE events fail. Observation success is always `product_acceptance: false` until
D evaluates the exact receipts against the sealed inventory.

## Independent review and future commands

The following commands are templates for a later allocation, **not permission
to execute a candidate**. The inert plan and mocked tests need no artifact,
service, network namespace, sockets, model, or credentials:

```sh
python3 scripts/qualification/installed_linux_offline.py plan
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s tests/recall_preview_semantics -p installed_linux_test.py -v
```

D reviews the cases and source, then creates its own review JSON with format
`recall-installed-linux-review/v1`, `reviewer: "qualification"`, `approved: true`,
and exact plan values for `expectations_sha256`, `corpus_sha256`, `driver_sha256`,
`collector_sha256`, and `installer_sha256`. These operator records are hashed
custody documents, not digital signatures. A change to any bound source requires
a new independent review before executing the changed harness.

The candidate must include the reviewed classified bootstrap implementation
(`31841b6` or its integrated successor). Missing named bootstrap credentials fail
before administrative requests; the driver never retries them with the global
operator. Policies remain `household-policy` level 3 and `business-policy` level 7.
Bootstrap token composition controls are offline mocks, not proof of CLI or
HTTP authorization; C's actual runtime tests and later installed receipts remain
separate requirements.

The allocator makes a run manifest from
`installed_linux_manifest.example.json`, fills trusted exact artifact/source/
binary/review digests, a new absolute canonical directory outside any project
or runtime, a short UTC deadline, and explicitly authorizes execution. Repeat
with separate directories/manifests for `elephant-db` and `surreal-db`.

After explicit root allocation, on Linux x86_64 with Python 3.12+, user/network
namespaces enabled, and the candidate's declared runtime libraries available:

```sh
recall_parent_netns=$(readlink /proc/self/ns/net)
unshare --user --map-root-user --net \
  python3 scripts/qualification/installed_linux_offline.py run \
  --manifest /absolute/allocated-run.json --execute-reviewed \
  --parent-network-namespace "$recall_parent_netns"
```

The parent namespace identifier must be captured in the real parent namespace.
Do not substitute a fabricated value or fall back to the host network when
unshare is unavailable. Use only the newly owned directories. Failed bootstrap,
missing runtime feature, migration failure, read-only precondition failure,
missing artifact, or digest mismatch is an unmet prerequisite/failure, never a
reason to set skip-auth or infer a pass from process health.

## Remaining independent gates

The inventory explicitly leaves source rendering/graph publication, semantic
relevance, installed provider dispatch, physical erasure and old-backup replay,
read admission during restore, SIGKILL recovery, remote patched SurrealDB,
container persistent volumes, Darwin, consumer adoption, and load unqualified.
OB3's physical deletion plus bounded backup-retention gate still requires its
own evidence; a successful restore of pending metadata cannot close it.

The controlled installed query-dispatch packet remains separate pending source
and D review. C identified the actual seam: retained MCP `search_entities` with
`use_vector: true`, nonempty query, first page, `limit: 1`, and `read:entity` calls
the approved query embedding path even in an empty space. REST entities search
is lexical and cannot supply this positive. The future transport-only packet
must create two distinct approved query/document profile pairs, two local spy
listeners and synthetic credential references inside the same isolated namespace;
measure exact outbound counts; then test missing/revoked approval and wrong-space
denial. Random, dimensions-valid vectors are explicitly spies, not provider
relevance. Existing Rust dispatch/index tests cover those actual internal seams;
their authorship or compile success is not installed-product execution.

The mocked Python tests below this directory check the harness's parser,
identity binding, ownership, privacy, budgets and cleanup. They do not mock a
successful Recall product run and cannot produce a qualification verdict.

The reusable Driver exposes source-owned `expected_target`, `observation_format`
and `create_process(arguments, log_paths)` for a separately reviewed platform
subclass. Linux defaults and namespace preflight remain unchanged. A manifest
cannot override the target or process implementation. The shared `process()`
method still enforces count, time, private-log and cleanup bounds. The alternate
platform owner must independently establish isolation and retain child-group
custody; these hooks do not certify Darwin or replace Linux `waitid` behavior.
# Shared process initialization and wait custody

The shared `OwnedProcess` constructor closes earlier private logs if a later log
open fails. After `Popen` succeeds, failure to create or start either log thread
triggers cleanup of the original owned process group, bounded log draining and
leader reaping. The original exception carries `owned_process` and
`owned_cleanup_errors`; a consumer must retain that handle when cleanup is
incomplete, even though construction raised. Active drainers retain their own
streams, so constructor cleanup does not block while closing a pipe they read.
No group signal is permitted after the original leader has been reaped.
If the final group signal fails, cleanup does not call the leader wait, even
when that leader has already exited. This preserves the original PID/PGID
reservation for a later cleanup attempt; successful leader reaping alone would
not establish that descendants were terminated.

The installed driver retains constructor-failure handles and ordinary command
handles separately from its active service. Cleanup visits every original handle
even if another handle fails to signal or reap. Handles remain registered until
reaped. All cleanup waits, including forced termination, consume the original
run and wall-clock allocation; expired allocations permit only zero-time waits.

Callers with an existing allocation pass `initialization_deadline` as an absolute
monotonic deadline. The installed driver supplies its original remaining run and
wall-clock allocation. The compatibility default is a finite five seconds, not
a fresh allocation for consumers with a stricter deadline. `wait(timeout)` uses
one deadline for process polling, every log-thread join and leader reaping; it
does not append fixed cleanup waits after that deadline. Timeout or failed
capture retains an incomplete result. Consumer cleanup verdicts must still
reject forced or nonzero persistent-service termination and inspect every owned
handle; this helper does not make such a cleanup acceptable.

Offline controls inject first/second thread-start failure, partial log-open
failure, signal/reap failure and exhausted shared deadlines with mocked children,
threads and signals. Separate controls cover a failed group signal with an
already-exited leader, concurrent service/failed-command custody, continued
cleanup after one handle fails, and zero-time waits on every retained handle.
They establish helper behavior only, not actual OS process
cleanup or installed qualification.

The Linux 19-case producer also records original process custody in a separate
versioned sidecar. Its independent review must pin `process_capture_sha256` in
addition to the existing driver, installer, collector and fixture identities.
The normal `run` CLI supplies the exact validated manifest bytes. Other platform
and dispatch adapters retain their own reviewed capture contracts.

Before each child launch, `ProcessProof` persists the original argument vector,
working directory, environment **names**, fixed deadline and executable identity.
It never reads environment values or credential files. `OwnedProcess` accepts
this optional proof through `capture_proof`; existing callers retain their
original custody behavior. Actual `/proc/PID/stat` bytes bracket executable
inspection while the original leader remains unreaped. PID, parent, process
group, session and birth ticks must agree. A fast CLI may already be a zombie,
with no readable `/proc/PID/exe`; that absence is recorded alongside its original
launch binary and raw zombie stat. It is never presented as a live executable
observation. The shared process object exposes the captured launch stat bytes
and executable descriptors for separately owned startup sidecars.

Actual signal requests and outcomes, original `waitid` exit observation, exact
reap, and each pipe's EOF/close/error are recorded at those boundaries. A signal
after exit observation is identified as descendant cleanup. Failed cleanup
retains the original unreaped handle and incomplete evidence. Logs gain complete
raw references only after actual EOF, closure and reap. No terminal verdict is
reconstructed from an exit code or a case-name list.

`captures/process-journal.json` uses `recall-installed-process-journal/v2`, with
`processes`, `attempted_processes`, `case_proofs`, `terminal_observed_ns`, and
`complete`. Each process contains `index`, `intent`, `launch_binary`, `identity`,
`events`, `errors`, `exit_observed`, `exit_code`, `reaped`, `reaped_ns`, and `logs`.
Each case additionally records the original request/process indices since the
previous case. All original HTTP records remain available, including failures;
request start, finish and deadline clocks are captured at execution.

`captures/process-evidence.json` uses `recall-installed-process-evidence/v2`.
Its fields are `controller`, `review`, `manifest`, `reviewed_plan`, `process_journal`, `cleanup`,
`capture_inventory`, `runtime_configuration`, and `artifact_manifest`. Each
present capture has its actual path and a `{sha256,bytes,kind}` raw reference.
The inventory follows `recall-dynamic-capture-inventory/v2`. Controller stat,
executable identity, boot ID, network namespace and raw routes/interfaces are
captured from this controller. No executable bytes or tokens enter the raw
inventory. Missing configuration/provenance stays absent on failed runs.
The reviewed-plan capture contains the original validated plan and references
to the original review, case-file bytes and corpus bytes. Their source hashes
are checked again before execution; the plan's helper pins remain explicit.

This sidecar supplies observations for D's independent
`recall-installed-offline-evidence/v2` carrier; it does not manufacture that
carrier's missing runtime/package/run identities. The original metadata recipe,
19-case outcomes and `product_acceptance:false` remain unchanged. A valid
current runtime may use separately approved provider bindings; the original
offline metadata run does not retroactively gain provider approval.

Metadata has a 2MiB sublimit and at most 512 original events, charged inside the
original 64MiB capture allowance with one shared lock and no counter reset.
The final observation is capped at 2MiB and charged to that same allowance.
Together with at most 16 pairs of 1MiB process logs, all original offline evidence
is bounded by 96MiB; executable/archive bytes are not part of this evidence map.
Existing process count, per-log, HTTP and 300-second bounds remain
in force. The allocation's wall expiry is also pinned to a monotonic deadline at
construction; every wait and cleanup uses the original bounds. Controller
capture retains the exact pinned nanosecond ceiling, while process, HTTP and
cleanup deadlines clamp to it after setup time. No tolerance or refreshed
allowance is required by the independent validator. The controller includes the
original `allocation_clock` with `mono_before_ns`, `wall_ns`, `mono_after_ns`,
`expires_wall_ns`, and `total_seconds`. Its one integer deadline is
`mono_before_ns + min(total_seconds * 1000000000, expires_wall_ns - wall_ns)`;
an allocation already expired by the second monotonic sample is refused.
The before sample makes the conversion conservative. Legacy float timeout
interfaces round down, while controller, process intents and cleanup retain
that exact original integer. The controller's later wall observation is
separately bracketed by `wall_sample_before_ns` and `wall_sample_after_ns`,
with `observed_ns` equal to the latter. Neither bracket renews the allowance.
Mock controls exercise producer mechanics with explicit process/proc/signal/thread doubles.
They start no child or service and establish no product or dynamic-component
acceptance.

Nested Python helpers are loaded from one bounded, regular, unsymlinked original
source read. The driver compiles those exact bytes with preserved module metadata;
it never asks a source loader to reread the file or execute cached bytecode.
The D fixture/parser modules are explicitly loaded from their original paths
before the collector, so an unrelated cached module cannot supply those imports.
Each loaded module instance retains its original source digest outside the
module. Plan construction compares that digest to the current helper file before
returning the existing review pins. Loading a changed file into another module
cannot make an older module pass this check. The original read-only runtime and
reviewed dependency inventory remain necessary; this does not add execution
authorization or change the installed plan's fields.

The shared process primitive also supports one explicit output exception for the
separately allocated full-workload controller. Its fixed command is
`snapshot inspect --input <canonical-absolute-bundle> --json --include-records --compact-records`.
The original Recall executable is launched directly. `ProcessProof` accepts
`stream_limits=(33554433, 1048576)` and
`stream_kinds=("snapshot-inspection", "process-log")`; `OwnedProcess` receives
the same `stream_limits` and its ordinary scalar cap remains 1048576. The report
bound includes the CLI's final newline. No other arguments, larger bound, role
alias or generic process-log exception is accepted.

Only this explicit invocation adds `streams` to the original prelaunch intent:
two rows, stdout then stderr, each with `path`, `kind`, `maximum_bytes` and
`reserved_bytes`. Both byte fields equal that stream's original fixed bound.
Before launching or creating either output file, the proof reserves their full
combined 34603009 bytes through the supplied journal's original shared capture
callback. The reservation is never refunded, including short output and failed
launches. The caller must not reserve those same two retained files again;
exports, additional copies and metadata remain charged to that same allocation.
Metadata retains its existing 2MiB/512-event sublimits. The caller supplies the
existing deadline and process-count limit, with no new allocation or clock.

The actual drainer applies the original per-stream bound. EOF, absence of capture
errors and an actual reap remain necessary for a complete reference. A report
that exceeds its bound retains an incomplete prefix and cannot qualify. The
original stdout reference uses `snapshot-inspection` throughout the process
journal, capture inventory and report consumer; its bytes never acquire a second
kind. Ordinary invocations keep their existing intent and four-MiB proof-read
ceiling, while the installed 19-case workflow still uses one-MiB logs and does
not invoke this exception. Snapshot custody alone does not establish physical
erasure, source correctness or qualification acceptance.

An enclosing reviewed loader may inject `VERIFIED_DEPENDENCIES` into this
module's globals before compiling its original source bytes. This internal
registry contains exactly `fixtures`, `matrix`, `oracle`, `installed_d_capture`,
`installed_release_artifact` and `installed_original_process_capture`. Each entry
has exactly `module`, `path`, `source_bytes` and `sha256`: the original module
object already executed by that loader, its canonical absolute source path,
the original bounded bytes used for that execution, and the reviewed digest.
The outer loader must prove that execution and supply the reviewed digest;
this registry is not a public input or a replacement for code provenance.

In this mode each dependency's name, module/spec path, byte length and hash must
agree before the same object is returned. Missing or mismatched entries refuse
without a filesystem-loader fallback. Neither ambient `sys.modules` entries nor
cached bytecode can provide a dependency. Hashes remain bound to the original
module instance, so modifying the registry cannot recertify its existing code.
The ordinary installed workflow continues to use its bounded original-source
loader. The injected mode performs no dependency-source reread or execution;
all transitive modules still require the enclosing reviewed loader's original
source and import-custody proof.

For a controller that must verify its helper source before importing it,
`pin_allocation_clock(..., original_sample=sample)` accepts the controller's
original pre-input `mono_before_ns`, `wall_ns` and `mono_after_ns` observations.
They must be positive integers, with the monotonic samples ordered. This path
reads no new clock: it copies those facts and applies the same deadline formula
and expiry-at-sample check. The caller must capture them before input reads and
retain the resulting original deadline through validation and execution. It
still checks remaining time before actions. Omitting `original_sample` preserves
the existing direct three-clock sample. Neither mode adds time or changes the
whole-run and backend allocation ceilings.
