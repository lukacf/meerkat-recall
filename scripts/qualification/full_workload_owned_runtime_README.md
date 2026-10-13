# Original full-workload runtime ownership

`full_workload_owned_runtime.py` is an inert library for the reviewed full-workload
CLI. It has no executable entry point and does not authorize execution. The CLI
loads the exact reviewed source and supplies the same `NamedClient`, `Capture`,
integer allocation clock, isolation facts and verified helper module objects.
The original limits remain 1,800 seconds per backend, 3,600 total, 8,000 HTTP
calls, one mutation at a time and 1 GiB of captures per backend. All activity is
the explicitly controlled synthetic spy workload. Independent acceptance is
still required.

The constructor accepts keyword-only `allocation`, `runtime_plan`, `client`,
`capture`, `deadline_ns`, `whole_deadline_ns`, `allocation_clock`, `isolation`,
`installed_helpers`, `process_capture`, `artifact_helpers`, `inspection_helpers`.
The latter objects are the original source-verified modules, not loader callbacks
or JSON assertions. `installed_helpers` is `installed_linux_offline`; its
`process_capture` and `artifact` objects must be the very same supplied objects.
The pure inspection helper is `snapshot_inspection_capture`. The enclosing CLI
must pin the complete executed dependency closure, including the six original
modules in F's `VERIFIED_DEPENDENCIES` carrier. This library imports only stdlib.

The runtime plan is a separately reviewed pre-output input:

| Field | Original input |
| --- | --- |
| `format` | `recall-full-workload-runtime-plan/v1` |
| `backend`, `run_nonce`, `endpoint` | Same backend, nonce and Recall endpoint as allocation |
| `spy_endpoint` | Reviewed distinct IPv4 loopback `/v1` endpoint |
| `run_directory` | New canonical direct child of the original capture directory |
| `archive`, `artifact_sha256` | Original canonical release archive and SHA-256 |
| `binary_sha256`, `source_commit`, `target` | Original reviewed installed identity |
| `paired` | `null` for embedded; otherwise exact `binary`, `binary_sha256`, `endpoint` |
| `spaces` | Both original named `CreateSpace` requests |
| `principals` | Each space's original `data`, `manage`, `revocable` subject/scope maps |
| `worker_runtime_expectations` | Complete independent queue, tree, embedding and factory expectations |
| `spy_credential` | Original fake `synthetic-controlled-provider-key`; never a real credential |
| `spy_credentials` | Optional exact per-space synthetic map for the headerless embedding workload |

The successor workload supplies `spy_credentials` for both original spaces.
Each entry has exactly `credential_ref` and `value`. The household entry uses
`FULL_WORKLOAD_SPY_KEY_QUALIFICATION_HOUSEHOLD` and
`synthetic-controlled-provider-key-qualification-household`; the business entry
uses the corresponding `BUSINESS` and `business` suffixes. These fixed fake
values cannot be replaced by arbitrary credentials. Every connection in a space
must refer to that space's key and the original owned spy endpoint. Children
receive only these two fake key variables; the legacy `FULL_WORKLOAD_SPY_KEY`
variable is absent on this path. The original `spy_credential` field remains
fixed as input provenance and for historical plans without the optional map.

The CLI and assembler bind every spy rule's credential digest to its declared
space before startup. Each credential/model/path tuple must identify one space;
embedding tuples must also identify exactly one rule. Generation may have
multiple source rules within its one bound space. Both actual generation and
embedding requests omit the space header. Capture preserves that absence as
`wire_space: null` and records the credential-derived space separately. A present
conflicting header is refused. No product header is added or inferred from text.

The CLI binds every plan field to the original allocation/case/spy inputs before
output, including the revocable subjects and unchanged spy credential digest.
The plan contains no future startup receipt, token, process ID or generated
configuration ID. The shared token map starts empty and is populated only by
the actual fixed `service-token` CLI writing new private files. The global
`runtime:operator` token has read/write system and status scopes;
`runtime:inspect` has read system only. Each named role receives its own actual
signed token and stored grant. No parent environment is inherited by children.

`async with owner` verifies the original offline namespace, refuses occupied
ports, verifies/extracts the real release into a fresh tree, and launches all
children through F's `ProcessProof` and `OwnedProcess`. Proof is prepared before
spawn. Native uses the reviewed paired server, a single explicit migration, then
Recall. Embedded requires an unconfigured readiness response followed by the
actual signed `initialize_runtime` operation. Both spaces and grants are created
and read back before workers start. Failed or cancelled construction retains
every actual child handle for cleanup. No arbitrary PID can be adopted.

The transport owner supplies `owner_request(method, path, body=None,
response_limit=...)` for its exact operator bootstrap/export allowlist,
`runtime_request(body)` for read-only C inspection, `bind_paired_health(endpoint)`
once before paired startup, and `paired_health(endpoint)` for its original
bounded readiness window. All share the original counters, captures and clock.
The existing `NamedClient` supplies exact `space:role` selection without fallback.

The original deadline reserves its final 75 seconds for cleanup. The public
`active_deadline_ns` is that earlier absolute bound; it never extends the
allocation. Startup internally respects it. The CLI must apply the same active
bound to Driver work and run context cleanup inside the original terminal bound.
The actual `cleanup_receipt`, `state == "stopped"` and empty `failures` are needed
for a clean terminal observation. At an already-expired original deadline,
cleanup truthfully reports unreaped handles; it does not manufacture a new
cleanup allowance or hide a forced shutdown.

`revalidate_original()` is active-only. It brackets actual original `/proc` stat
and executable capture and returns the stable projection agreed with C:
`recall-full-workload-process-identity/v1`, backend, nonce, and Recall/paired
executable SHA-256 plus original pid/ppid/pgrp/session/start_ticks. Raw current
facts are retained separately in the same capture. C compares this across all
four independent inspection boundaries. `stop_original(backend, nonce)` checks
those original handles and facts before signaling Recall. A clean result needs
exit zero, reaping, complete EOF and no capture errors; the paired server remains
alive until final context cleanup. No restart is provided.

`capture_original_export(backend, nonce, observed)` binds the actual completed
Graph source/operation/generation/desired revision and full raw/rendered hashes.
It calls the real quiesced snapshot endpoint, reserves 64 MiB for each original
payload, verifies the owned manifest/payload and invokes the same installed
binary with fixed `snapshot inspect --input ... --json --include-records
--compact-records` arguments. F's original pre-spawn stream role reserves
33,554,433 bytes for stdout and 1 MiB for stderr from the same capture, without
refund or duplicate charging. Incomplete/truncated output is never successful.
The original stdout reference retains kind `snapshot-inspection` everywhere.
F's shared pure `checked_inspection(..., expected_format=...v2)` checks complete
report custody; D remains the sole physical decoding/acceptance oracle. The two
exports and every metadata/log file share the original 1 GiB budget.

The native path uses exactly sixteen processes: key bootstrap, two global
tokens, eight named tokens, paired server, explicit migration, Recall and two
inspections. There is no process allowance for token refresh, retries, an extra
version probe, or a replacement service. Ordinary logs retain the 1 MiB per
stream bound. F's metadata journal remains 2 MiB/512 events.

`full_workload_owned_runtime_test.py` uses temporary files and explicit fake
process/proof/HTTP/helper objects. It forbids real subprocess and connection
creation. Controls cover original handles and stat identity, backend/nonce,
deadline/capture refusal, actual constructor failures and cleanup, the exact
native process count, explicit embedded initialization, full publication hashes,
payload changes, incomplete report refusal and retained paired-server lifetime.
These controls are source/custody checks only; they do not qualify an installed
artifact, service, actual snapshot, provider, deletion, or workload.

Cancellation does not leave an unowned background `wait`: the original bounded
wait task is retained and joined before cleanup may reap again. All process
methods that can acquire F's group/proof locks run outside the event-loop thread;
shared-capture reservations are serialized back onto that thread. Launch proof
hashing uses the earlier active deadline, and constructor initialization uses
its intersection with the existing five-second initialization bound. Neither
can consume a newly minted cleanup window.

Current executable observations also hash against that original active deadline.
Every background capture, process method, bounded file read, installation and
constructor retains its original thread task until it finishes before cancellation
can unwind the owner. Repeated cancellation cannot detach the same writer while
cleanup records terminal evidence. Held-observer and held-journal-save controls
exercise cancellation before release and verify the original writes finish before
cleanup begins; no process or network action is used in those controls.

Isolation input retains the existing `recall-owned-network-isolation/v1` shape,
including `original_host_network_namespace` and `current_network_namespace`.
There is no translated replacement receipt. The backend and nonce must match;
F's actual current namespace observation must equal the original current field
before any directory, artifact or process action.
