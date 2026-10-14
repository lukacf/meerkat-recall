# Original-allocation Darwin bootstrap

`scripts/release/supervise_darwin_build.py --bootstrap` supplies the missing
acquisition and tool-provisioning entry to the existing supervisor. It uses the
same `Supervisor.command`, process-group observer, durable lane marker, original
150 GiB ledger, 250 GiB floor, four-job setting, 96 GiB RSS ceiling and original
START expiry as the build. It never opens a replacement allocation. The timer
also retains the entry monotonic/wall sample through preparation; late phases
consume the original remaining time, including fifteen seconds for cleanup.

The normal build CLI continues to require the final detached source and pinned
task-local Python 3.12.12/Rust 1.94.0. Bootstrap instead uses an existing trusted
native ARM64 Python 3.11 or newer with its actual canonical path and SHA256. The
bootstrap helper closure is `supervise_darwin_build.py`, `darwin_bootstrap.py`,
`darwin_build_process.py`, `darwin_build_child.py`, `artifact.py`, and
`container_artifact.py`; transfer and verify these exact reviewed bytes before
launch. Importing them does not start an observer, listener or process.

The closed schema is [darwin-bootstrap-allocation.example.json](darwin-bootstrap-allocation.example.json).
Its false authorization and placeholder tool receipts deliberately prevent
execution. Keep the original START grant, timestamps, ledger path, monitored
filesystem set and lock. Create fresh private task-local receipt/acquisition
folders, and bind the actual existing interpreter and manager paths. Every tool
requires an independently reviewed private origin receipt of this shape:

```json
{"format":"recall-darwin-trusted-tool/v1","reviewed":true,
 "executable":"/actual/invocation/tool","canonical_executable":"/actual/resolved/tool",
 "sha256":"ACTUAL_SHA256",
 "origin":"Actual manager/install provenance reviewed by the Mac operator"}
```

Manager invocation paths retain their argv[0] (notably `rustup` pointing to
`rustup-init`); the origin receipt binds both invocation and canonical executable
bytes. The bootstrap interpreter itself uses its canonical executable.
The rustup version query alone uses that same pinned binary's canonical
`rustup-init --version` path, whose setup-mode version branch returns before
installation. The explicit installation still uses the `rustup` invocation and
minimal profile. `RUSTUP_AUTO_INSTALL=0` disables implicit toolchain installation;
existing installed components and cumulative consumption are retained.
See the pinned [rustup 1.28.2 setup-mode source](https://github.com/rust-lang/rustup/blob/1.28.2/src/cli/setup_mode.rs#L90-L95).

Each allocation tool reference contains `path`, `sha256`, and
`origin_receipt:{path,sha256}`. Tools mode requires exactly `uv` and `rustup`.
Source mode requires `git` and exactly the transport for its location: `gcloud`
for `gs://`, or `curl` for HTTPS; combined mode also requires `uv` and `rustup`.
The reviewed Git invocation must be named `git`; its exact parent leads the
helper PATH, and resolution is checked before the artifact identity helper can
run. No arbitrary command list, shell string, inherited credential environment,
Homebrew installation or Apple developer selection is accepted.

After the exact original allocation is filled and reviewed, the operator uses:

```sh
"$reviewed_bootstrap_python" scripts/release/supervise_darwin_build.py \
  --bootstrap --allocation /absolute/private/bootstrap-allocation.json \
  --sha256 EXACT_REVIEWED_ALLOCATION_SHA256 --plan
"$reviewed_bootstrap_python" scripts/release/supervise_darwin_build.py \
  --bootstrap --allocation /absolute/private/bootstrap-allocation.json \
  --sha256 EXACT_REVIEWED_ALLOCATION_SHA256
```

`--plan` validates existing paths, provenance and schema without acquiring the
lane or starting a process. Execution samples the original resource budget
before every acquisition command and throughout each owned group at five-second
intervals. Manager commands use fixed task-local destinations, `uv --no-config
python install --no-bin cpython-3.12.12-macos-aarch64-none` and `rustup toolchain
install 1.94.0-aarch64-apple-darwin --profile minimal --no-self-update`. They retain
manager version/install logs and actual resulting interpreter/compiler hashes.
These are observed installed identities; a version label does not independently
prove a downloaded archive checksum. The manager origin receipts and retained
installation evidence remain available for review.

`operation:"tools"` has `source:null` and can prepare tools while the final
source is pending. `source` or `tools-and-source` requires:

```json
{"url":"gs://exact-reviewed-bucket/final.bundle","bundle_sha256":"ACTUAL_SHA256",
 "cloudsdk_config_directory":"/actual/existing/private/gcloud-config",
 "source_identity":{"source_commit":"ACTUAL_40_HEX","source_tree":"ACTUAL_40_HEX",
 "source_tar_sha256":"ACTUAL_64_HEX","cargo_lock_sha256":"ACTUAL_64_HEX"}}
```

The GCS adapter executes only the pinned `gcloud storage cp EXACT_URI EXACT_DEST`
through the original supervisor, with a 1210-second command ceiling clamped to
the original allocation. Its location names one literal object; wildcards,
generation selectors, credentials, queries and fragments are refused. The
explicit canonical, private, existing `cloudsdk_config_directory` selects the
Mac operator's existing authenticated configuration. No new credentials or
signing workflow is introduced, and credential contents are never copied into
receipts. Gcloud uses its installed SDK launcher and interpreter selection; the
launcher hash does not attest its full Python/module dependency tree. It uses
`storage/process_count=1` and `storage/thread_count=1`, with sliced downloads
disabled and tracker files in the task acquisition directory. Prompts, update
checks, usage reporting, HTTP logging and file logging are disabled. These are
per-command environment properties, not changes to the existing configuration.
See Google's [configuration properties](https://cloud.google.com/sdk/gcloud/reference/config/set)
and [interpreter selection](https://cloud.google.com/sdk/gcloud/reference/topic/startup).

For direct HTTPS instead, set `cloudsdk_config_directory:null` and select `curl`.
Query tokens and embedded credentials are refused. Curl refuses redirects and
sets a 128 MiB/1200-second download bound. Both transports have their exact
output checked for size and type in the existing five-second resource sample;
this sampled check can observe an overshoot and is not an instantaneous file
quota. The original allocation/floor remains enforced. A complete bundle must
be at most 128 MiB with the exact pinned hash before Git initialization/fetch.
Bundle verification, exact HEAD,
detached checkout, no alternates, full strict fsck and the reviewed bootstrap
artifact helper's complete source identity run under the original command
supervisor. Downloaded source Python is not executed for identity verification.
Git output and the exact final identity are retained.

A separate later source/bootstrap/build phase must retain this same original
START and expiry, cumulative ledger and lane. Its receipt directory must be fresh.
There is no automatic retry. After confirmed group cleanup the supervision
receipt is committed before the durable lane marker is cleared. An unresolved
owned group or capture retains the marker. Bootstrap success never asserts a
build, inference, semantic qualification or product acceptance. Use actual tool
and source receipts to fill the original build allocation; no prepared state is
fabricated by this mode.

`operation:"native-controls"` also requires `source:null`, with exactly the
existing trusted `rustup` tool and a fresh receipt directory. It uses the same
`--bootstrap --allocation ... --sha256 ...` CLI, original `Supervisor.execute`
lane/resource ownership and `Supervisor.command`. Its fixed commands are the
pinned Python immediate exit (five seconds), an `os.fork` fixture whose exited
leader leaves one same-group child (five seconds), and the pinned canonical
`rustup-init --version` (thirty seconds). Each keeps the existing fifteen-second
cleanup reserve; the control phase further clamps the original deadline to at
most 120 seconds without changing START/expiry or ledger. It never invokes the
tool installer or source acquisition. The descendant handshake and JSON log
bind actual leader/PGID/descendant identities to the durable command receipt.
All three exact commands require returncode zero and confirmed group/capture
cleanup before `native-control-observation.json` can report success. EPERM and
custody-write fault branches remain separately covered by the selected mocked
controls; this native mode does not claim that a fault occurred naturally.
Historical missing-custody administrative retirement remains separate from
native control success and does not become retrospective cleanup proof.
The descendant case additionally requires its actual PID in the retained first
native cleanup sample, a successful signal to the original group before leader
reaping, and the final complete empty sample. An externally vanished descendant
does not satisfy that case. The immediate-exit case requires actual NOTE_EXIT.

Before releasing each child, a private, fsynced `NAME.custody.json` records its
original PID and expected process group. Final command records retain that
identity, cleanup stage, last native group sample and any signal errno even if
cleanup fails. Cleanup observes the unreaped original group before signalling.
An EPERM race requires a fresh complete sample with no live or uncertain member;
the errno alone never proves cleanup. The leader is reaped only after that
sample, and its numeric group is never signalled after reaping. A prior failed
receipt without a child identity cannot be retroactively repaired by this
change; its marker requires separate native operator reconciliation.
