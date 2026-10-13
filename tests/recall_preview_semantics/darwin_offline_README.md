# Native Apple Silicon acceptance

`scripts/qualification/darwin_offline_native.py plan` is inert. It imports the
shared installed driver and prints the exact inherited case inventory plus every
transitive source digest. It performs no candidate execution, socket operation,
credential discovery, model access, build or service installation.

Execution needs a separate expiring root allocation and an independent
`qualification` review. The example manifest deliberately fails validation.
Review the final joined source, including shared helpers; an older review does
not authorize a changed dependency. Both embedded and local RocksDB use the same
cases in separate fresh directories. No remote database is used.

The subclass inherits the actual CLI authentication, classified household3 and
business7 spaces, current grants/revocation, HTTP/MCP parity, pending source
coverage, acknowledged snapshots, original-head restore, reopen and ReadOnly
cases from `installed_linux_offline.Driver`. It does not fork their assertions.
The shared bootstrap requires the actual supported `--bootstrap-space` CLI:
global management, classified bootstrap and ordinary current-grant service
authority remain separate. No generated token is printed in observations.

The original shared `unqualified_gates` array is preserved verbatim. A separate
`platform_cases` result reports only this Darwin execution subset; even a passed
run keeps `product_acceptance: false`. Neither providers, semantic relevance,
physical erasure, crash recovery, production adoption nor general macOS
compatibility is qualified here.

## Platform custody

The prebuilt archive must have target `aarch64-apple-darwin`, exact trusted
archive/source/binary identity, and a reviewed compatibility receipt. The
initial receiving environment is macOS 26.5.2 arm64. A new OS needs an explicit
new allocation and actual minimum-version evidence. The receipt format is
`recall-darwin-compatibility/v1`; fields are `source_commit`, `archive_sha256`,
`binary_sha256`, `architecture`, `receiver_os_version`, `binary_min_os`,
`onnx_min_os`, `binary_load_commands_sha256`, `onnx_archive_sha256`, and
`onnx_load_commands_sha256`. These are custody records from the builder's actual
`otool -l` / static ONNX archive inspection, not declarations inferred from CPU
architecture. The driver validates their binding and refuses a newer minimum OS.
The operator retains the original command output behind each digest. The agreed
minimum-version ceiling is 26.5 for both the executable and all linked ONNX
objects. For multiple ONNX archives, the archive digest binds the builder's
canonical JSON inventory of exact filenames/SHA256 values; original archives and
the complete combined load-command output are retained with that inventory.

The allocated canonical Python 3.12+ interpreter must be the current interpreter
and match its trusted digest. No interpreter is downloaded. The private child
environment contains no ambient auth, proxy, model or provider settings. Private
HOME, TMPDIR, snapshot and original recovery-journal paths are inside a new
canonical directory outside existing project/runtime directories. Skill lookup
uses the archive's actual `installed/skills` directory.

Every candidate runs under the existing `/usr/bin/sandbox-exec`, with a newly
written default-deny Seatbelt policy. It grants read access to the installed
artifact and required system runtime, writes only to six synthetic mutable
directories, and permits bind/inbound only on one owned loopback port. There is
no outbound rule, process-fork permission, keychain/Mach service access, ambient
HOME read, installed-tree write or observation write permission. The trusted
parent writes private evidence using already bounded collectors.

Before executing Recall, a separate trusted interpreter probe uses the same
write/network rules with its own interpreter/library read allowance. It must
write/read an allowed synthetic canary, bind/listen on the exact intended service
port, fail reading and writing synthetic files outside allowed paths with
EPERM/EACCES, and fail connecting to a parent-owned loopback listener with the
same errno. The listener must receive no connection. No nonlocal address is
probed and no production file is touched. Candidate policy has fewer executable
and read permissions than the probe. Missing or ineffective isolation reports
UNMET and starts no candidate. The host sandbox configuration is never changed.
Any additional runtime permission required by a real host needs source review;
there is no unsandboxed fallback.

The trusted child waits on a release pipe while the parent registers the native
kqueue NOTE_EXIT event, then execs sandbox-exec and the exact candidate. This
avoids the edge-triggered registration race for short CLI commands. The original
session leader remains unreaped through group cleanup; a lock excludes signals
after final reap. Cleanup only targets owned retained handles, never process
names, discovered host PIDs or a previously reaped identifier. Seatbelt denies
forking; candidate runtime threads remain allowed.

These choices follow the actual [XNU process event implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_event.c),
[Python kqueue interface](https://docs.python.org/3.12/library/select.html#kqueue-objects),
and [XNU process resource ABI](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/proc_info.h).
Seatbelt policies are generated for this harness; syntax and operation names
were checked against [Codex's macOS sandbox implementation](https://github.com/openai/codex/tree/main/codex-rs/sandboxing/src).

## Resource limits and evidence

There is one service at a time, at most 16 shared CLI/service children plus one
isolation probe, and 300 seconds for the inherited suite. Children use nice 10,
two Tokio threads, two Rayon threads, no core files, 1024 descriptors and a hard
180 CPU-second limit each. No Cargo, daemon, launchd, Homebrew, model cache or
host security setting is touched.

A continuous one-second monitor samples the actual owned root (including
verification extraction), free space and the unreaped child's RSS using
`proc_pidinfo`. It terminates owned children on a 4 GiB root/RSS limit, fewer than
8 GiB free, 100,000 filesystem entries, deadline expiry or unavailable resource
observation. The archive has separate 2 GiB compressed/declared expansion bounds
checked before shared extraction. Logs and HTTP captures retain the shared
byte/count/deadline bounds. The final receipt records sample count, peak root
bytes/RSS and minimum free bytes.

Disk/RSS limits are sampled and can overshoot between samples. macOS offers no
Linux cgroup or CPU-affinity enforcement here; thread counts and CPU time do not
provide a hard host CPU-share cap. The operator must allocate this small run
alongside HomeCore and retain its external stop authority. The runner leaves
only its own private evidence/data directory and never prunes foreign caches.

After source review, the independent review JSON contains format
`recall-darwin-native-review/v1`, `reviewer: "qualification"`, `approved: true`,
and the exact `platform_expectations_sha256`, `driver_sha256`, and
`dependency_sha256` from the inert plan. Run with the reviewed interpreter:

```sh
/absolute/reviewed/python -I scripts/qualification/darwin_offline_native.py run \
  --manifest /absolute/allocated-run.json --execute-reviewed
```

Exit 78 means an unmet prerequisite, exit 1 a failed observation/control, and
exit 0 means only the inherited installed subset passed and still requires an
independent verdict. `observation.json` and captures are private. Mocked controls
in `darwin_offline_test.py` exercise policy construction, exact review binding,
archive bounds, non-reaping event custody, cleanup order and resource refusal;
they execute no candidate and prove no actual Darwin sandbox behavior.

The platform constructor uses the shared process helper's bounded initialization
cleanup and exception-attached original child handle. A failed log thread or
launch handshake cannot drop custody, including for direct isolation probes.
Failure to signal the original group retains its unreaped leader and kqueue.
Initialization, probe cleanup, final child cleanup and monitor joins consume the
original run and wall-clock allowance; expiry never adds a fresh cleanup wait.
