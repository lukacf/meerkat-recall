# Installed Linux durability faults

This driver supplies bounded installed-candidate evidence for D22/D23. Running it
without arguments only prints the reviewed case inventory and source hashes. It
performs no build, package installation, provider request or deployment. Source
and mocked tests are not installed qualification.

The root scheduler allocates a verified Recall archive and, for the authentication
negative, a matching reviewed paired native bundle. The allocation binds the
source commit, archive and binary hashes, expiry, fresh external run directory,
and an independent `qualification` review of the exact driver, cases and seven
transitive helper/input files. The example allocation is deliberately disabled.
The review format is `recall-installed-fault-review/v1`; copy the plan's
`expectations_sha256`, `driver_sha256` and `dependency_sha256` into the independently
reviewed record with `reviewer: qualification` and `approved: true`. A paired bundle
also requires `paired_binary_sha256`, `paired_source_sha256` and
`paired_build_sha256` bound to its actual files. No downloaded or ambient server
is discovered.

Prerequisites are Linux, Python 3.11 or later, unprivileged user/mount/network/PID
namespaces, `bwrap`, `setpriv`, and permitted tracing of an owned child with
`strace`. The syscall collector is specific to the installed ENOSPC cases. If
tracing is unavailable, those cases remain unmet; deterministic library tests
remain separately useful. No ptrace capability is added to the product. Namespace
creation, artifact execution and faults occur only with `--execute ALLOCATION`.

The driver verifies and installs the archive into the new private run directory,
then launches itself as the namespace keeper. Only that directory is writable
on the host. The candidate's configuration, checkpoint and WAL use a private
64 MiB tmpfs. The independently retained original journal, synthetic credentials
and observations live outside that quota. There is no host mount or host disk
fill. The keeper survives candidate SIGTERM/SIGKILL and holds the same filesystem
across reopens. The isolated loopback is its only network interface; candidate
processes drop all capabilities and inherit two CPU affinity slots, four Tokio
workers, two Rayon workers, a 4 GiB address-space ceiling and disabled core dumps.

The actual public MCP/HTTP flow creates a named space with empty processing
bindings, workers disabled, a management-only global credential and a separate named-space data credential
issued by the candidate CLI. The operator creates the current service grant;
only the scoped service credential creates or reads evidence records. One acknowledged entity is checkpointed by clean shutdown; a
second is acknowledged only in the WAL. Before faulting, evidence records the
exact configured paths, checkpoint sequence/store/journal identity, digests and
complete acknowledged entity responses. Filling one newly owned file until
kernel error 28 exhausts only the quota. Independently, the candidate's traced
write to its exact configuration temporary-file directory and checkpoint
recovery directory must return ENOSPC. A generic nonzero exit, permission error,
or a filler error alone does not pass either save case.

Failed configuration must preserve the prior exact TOML bytes. Failed shutdown
checkpoint must exit nonzero and preserve the prior checkpoint, acknowledged WAL
and original journal. The driver removes only its filler, reopens the same path
and requires exact acknowledged records after recovery readiness. A subsequent
SIGKILL occurs after a third acknowledgment and observed WAL change with unchanged
checkpoint. Its evidence states that observed phase; it does not claim that the
kill occurred inside a particular atomic-write syscall.

D23 checks actual nonzero CLI initialization refusal for an owned non-listening
loopback endpoint, unsupported transfer of the candidate's actual live checkpoint,
and wrong synthetic credentials against the matching isolated paired server.
The credential case first requires successful native migration with the correct
credential. No failed case may emit initialized-success or publish runtime
configuration; unsupported transfer additionally preserves the source hash and
leaves its target absent. A missing paired bundle is an unmet prerequisite,
not a passing credential result.

`observations.json` retains per-case pass, fail or unmet-prerequisite verdicts.
Unexecuted cases are explicit and prevent `installed_cases_passed`. All reports
retain `product_acceptance: false`: actual atomic-writer phase injection and fresh
canonical `apply_files` valid/invalid-file rollback tests must be attached as
separate evidence. An extension failing after canonical commit does not prove
canonical rollback. None of this establishes semantic, provider, Darwin, OB3 or
production-cutover acceptance.

Bounds are 600 seconds plus cleanup, 32 owned child processes, 150 HTTP calls,
1 MiB per HTTP body, 2 MiB per child output stream, 8 MiB per sampled private
syscall trace, and 16 MiB of structured observations. Process groups remain owned
until reaped. The exact traced candidate uses a pidfd for shutdown so the collector
can complete its capture. Trace polling can overshoot by one 25 ms interval; it is
not a kernel file-size quota. Cleanup terminates only owned children and preserves
private evidence and the original journal; no foreign cache or resource is pruned.

Offline validation:

```sh
python3 tests/recall_preview_semantics/installed_linux_faults_test.py
python3 scripts/qualification/installed_linux_faults.py
```
