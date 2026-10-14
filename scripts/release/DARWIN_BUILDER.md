# Apple Silicon candidate build handoff

This is an operator recipe for an allocated native builder. It does not reserve
the shared Mac build lane. The coordinator must freeze the reviewed source and
obtain START from `codex-security-adr-mac` before any Cargo invocation; release
that lane only after the build and its children have exited. Use one build at a
time, including retries. Do not build on the live HomeCore service machine.

The candidate uses Rust **1.94.0**, Python **3.12.12**, Apple CLT/SDK and libclang,
package `meerkat-recall-bin`, defaults plus `rocksdb,fastembed`, and the existing
`deploy` profile. The recipe defaults to four Cargo jobs and preserves all
supported features. Neither a workspace build nor a `--release` build produces
the reviewed recipe. The latter enables full LTO and has different memory use.

Prepare an absolute, canonical, new task directory outside another project or
runtime. The examples below use shell variables whose concrete values belong in
the coordinator's allocation; angle-bracket values are placeholders, not commands.
All receipts, logs, downloads and output go outside the clean source checkout.
Keep the directory and lane unchanged for retries so cached work remains useful.
Do not delete another job's cache or change global toolchain/SDK configuration.

## Exact source transfer

On the source machine, after the final integration freeze and without building:

```bash
set -euo pipefail
recall_source=/absolute/reviewed/checkout
recall_transfer=/absolute/new/transfer-directory
recall_frozen=REVIEWED_FULL_40_HEX_COMMIT
mkdir -m 700 "$recall_transfer"
test "$(git -C "$recall_source" rev-parse HEAD)" = "$recall_frozen"
test -z "$(git -C "$recall_source" status --porcelain --untracked-files=normal)"
python3 "$recall_source/scripts/release/artifact.py" source-identity --root "$recall_source" \
  > "$recall_transfer/source-identity.json"
git -C "$recall_source" bundle create "$recall_transfer/source.bundle" HEAD
shasum -a 256 "$recall_transfer/source.bundle" \
  > "$recall_transfer/source.bundle.sha256"
```

Transfer the bundle and identity receipt through the agreed channel. Communicate
the full source commit and bundle digest independently. A plain source tarball
loses the Git identity required by `build-native`; do not invent a replacement
commit. The bundle preserves the reviewed commit, tree, vendored sources and
executable modes. No production data, credentials or build cache is transferred.

On the allocated Mac, verify the trusted bundle digest before cloning. Use the
same account that owns the allocated task and build cache:

```bash
set -euo pipefail
recall_task=/absolute/new/recall-task
recall_bundle=/absolute/transferred/source.bundle
recall_frozen=REVIEWED_FULL_40_HEX_COMMIT
recall_bundle_sha256=TRUSTED_FULL_64_HEX_DIGEST
mkdir -m 700 "$recall_task"
recall_task="$(cd "$recall_task" && pwd -P)"
test "$(shasum -a 256 "$recall_bundle" | awk '{print $1}')" = "$recall_bundle_sha256"
mkdir -m 700 "$recall_task/receipts" "$recall_task/output" "$recall_task/tmp" \
  "$recall_task/tools" "$recall_task/uv-cache" "$recall_task/rustup" \
  "$recall_task/cargo-bootstrap" "$recall_task/ort-cache"
git clone --no-checkout "$recall_bundle" "$recall_task/source"
git -C "$recall_task/source" checkout --detach "$recall_frozen"
git -C "$recall_task/source" bundle verify "$recall_bundle" \
  > "$recall_task/receipts/git-bundle.txt" 2>&1
test "$(git -C "$recall_task/source" rev-parse HEAD)" = "$recall_frozen"
test -z "$(git -C "$recall_task/source" status --porcelain --untracked-files=normal)"
```

## Task-local prerequisites

Use the builder's existing trusted `uv` and `rustup` executables. Installation
below downloads tools into this task; it does not select a default toolchain,
write shell profiles, install Homebrew packages or change `xcode-select`.
The local `uv` must support `--no-bin` and the pinned ARM64 Python request. Record
its version, download URL and resulting interpreter digest. If unavailable,
resolve the prerequisite explicitly instead of selecting a different Python.

```bash
recall_uv="$(command -v uv)"
recall_rustup="$(command -v rustup)"
"$recall_uv" --version > "$recall_task/receipts/uv-version.txt"
"$recall_rustup" --version > "$recall_task/receipts/rustup-version.txt"
export UV_PYTHON_INSTALL_DIR="$recall_task/tools/python"
export UV_CACHE_DIR="$recall_task/uv-cache"
"$recall_uv" --no-config python install --no-bin cpython-3.12.12-macos-aarch64-none
recall_python="$("$recall_uv" --no-config python find --managed-python --no-python-downloads --no-project 3.12.12)"
recall_python="$("$recall_python" -c 'import pathlib,sys; print(pathlib.Path(sys.executable).resolve())')"
"$recall_python" -c 'import platform,sys; assert sys.version_info[:3] == (3,12,12); assert platform.machine() == "arm64"; print(sys.version)'
shasum -a 256 "$recall_python" > "$recall_task/receipts/python.sha256"
export RUSTUP_HOME="$recall_task/rustup"
export CARGO_HOME="$recall_task/cargo-bootstrap"
"$recall_rustup" toolchain install 1.94.0-aarch64-apple-darwin --profile minimal --no-self-update
export RUSTUP_TOOLCHAIN=1.94.0-aarch64-apple-darwin
recall_toolchain="$RUSTUP_HOME/toolchains/$RUSTUP_TOOLCHAIN/bin"
export PATH="$(dirname "$recall_python"):$recall_toolchain:/usr/bin:/bin:/usr/sbin:/sbin"
python3 --version > "$recall_task/receipts/python-version.txt"
rustc -vV > "$recall_task/receipts/rustc.txt"
cargo -V > "$recall_task/receipts/cargo.txt"
test "$(rustc --version | awk '{print $2}')" = 1.94.0
test "$(rustc -vV | sed -n 's/^host: //p')" = aarch64-apple-darwin
```

The selected CLT must supply clang, clang++, make, ar, a usable SDK, and libclang
for RocksDB bindgen. Record these actual paths and versions; the known builder
reports CLT/SDK 26.5 and libclang. `LIBCLANG_PATH` below must be its verified
absolute library directory. Merely finding `clang` does not establish libclang.
Do not run `xcode-select --switch` or install a second SDK as part of this recipe.

```bash
export DEVELOPER_DIR="$(xcode-select -p)"
export SDKROOT="$(xcrun --sdk macosx --show-sdk-path)"
export CC="$(xcrun -f clang)"
export CXX="$(xcrun -f clang++)"
export LIBCLANG_PATH=/absolute/reviewed/CLT/libclang-directory
test -f "$LIBCLANG_PATH/libclang.dylib"
sw_vers > "$recall_task/receipts/builder-os.txt"
xcrun --sdk macosx --show-sdk-version > "$recall_task/receipts/sdk-version.txt"
"$CC" --version > "$recall_task/receipts/clang.txt"
"$CC" --print-search-dirs > "$recall_task/receipts/clang-search.txt"
```

No `protoc`, audio SDK or ONNX source compiler is required by this locked selected
graph. `LZMA_API_STATIC=1` compiles the bundled C source. ort-sys downloads its
checksum-pinned native ARM64 ONNX Runtime distribution. Neither downloads model
weights nor authorizes an external provider. Locked crates and native runtime
downloads require access or exact verified warm caches.

The source-only [dependency inventory](darwin-build-prerequisites.json) records
reviewed lockfile crate checksums, relevant cached build-script hashes, and the
selected recipe. Reconcile its input hashes after the final source freeze; it is
not an executed Cargo dependency report or a builder observation.

| Prerequisite | Actual selected graph / remaining evidence |
| --- | --- |
| Apple CLT clang/clang++, ar, linker, SDK, libclang | RocksDB `surrealdb-librocksdb-sys` 0.18.3+11.0.0-4 uses `cc` and runtime bindgen. Compression, regex and crypto crates compile bundled C/C++/assembly. Record actual CLT paths/versions; libclang must load on this host. |
| Existing system tools | Bash, Git, shasum, sed, awk, cut, basename, dirname, find, mktemp, cmp, nice, uname, xcrun, xcode-select, otool, make and ordinary file utilities. The recipe needs no Homebrew tool substitution. |
| Rust 1.94.0 native ARM64 | Exact minimal toolchain, invoked from task-local real compiler/Cargo binaries; the wrapper avoids rustup shims that could install the repository's optional development components. Retain rustup distribution verification and downloaded component receipts. |
| Python 3.12.12 native ARM64 | Existing trusted uv must support the pinned request and `--no-bin`. Retain selected download URL/hash and actual interpreter SHA256; a version label alone is not a download identity. |
| Locked Cargo crates | Existing Cargo registry access or exact warm cache; `--locked` and Cargo.lock checksums remain authoritative. No manifest or feature reductions. |
| Native ONNX Runtime | `fastembed` 5.17.2 selects ort-sys 2.0.0-rc.12 download/native TLS. The exact 1.24.2 ARM64 URL/SHA below is required. Only the task-local ORT cache location changes. No model weights are fetched by the build. |
| TLS and native libraries | Darwin native-tls selects Apple Security frameworks, not the non-Apple OpenSSL dependency in the shared lockfile. Do not infer a Homebrew OpenSSL requirement from its mere lockfile presence. Actual packaged dynamic links must remain system-only. |
| CMake/protoc/audio/Perl | CMake, protobuf build packages, whisper and CPAL are absent from this lockfile. Ring's registry package uses pregenerated assembly. No source-generation Perl, ONNX source build or audio SDK is added by this recipe. |

Remaining prerequisites are concrete operator inputs: trustworthy existing uv and
rustup provenance, actual downloaded tool identities, exact CLT/libclang/SDK
paths, the final source identity, and the granted lane/ledger allocation. This
inventory installs nothing and does not establish successful compilation.

The confirmed builder **Lukas-BigMac** and receiving HomeCore service machine both
run macOS **26.5.2 arm64**, with SDK **26.5**. The recipient reports CLT **26.6**;
record the builder's actual CLT separately. The receiving team accepts an actual
minimum deployment version at or below **26.5**. These facts define this one
handoff, not a support promise for other Macs. Do not add an ambient deployment
override: inspect the actual resulting Mach-O minimum versions below. If either
the executable or linked ONNX object requires a higher version, stop and review
the build contract before changing compiler flags.

## Cache allocation and one build

The repository wrapper overrides ambient `CARGO_HOME` and `CARGO_TARGET_DIR`.
It creates a repository-specific Cargo home and lane-specific target under the
real user's `~/Library/Caches/rust-workspaces`. Keep `HOME` unchanged. Record the
actual paths before allocation; do not estimate from a shell variable that the
wrapper replaces. The task-local Rust toolchain remains under `RUSTUP_HOME`.

```bash
cd "$recall_task/source"
export RUST_LANE_ID="recall-darwin-${recall_frozen:0:12}"
export CARGO_BUILD_JOBS=4
export RECALL_BUILD_MIN_FREE_GIB=250
export CARGO_INCREMENTAL=0
export LZMA_API_STATIC=1
export TMPDIR="$recall_task/tmp"
export ORT_CACHE_DIR="$recall_task/ort-cache"
./scripts/repo-cargo --print-env > "$recall_task/receipts/cargo-env.txt"
python3 scripts/release/artifact.py source-identity > "$recall_task/receipts/source-before.json"
```

Compare the complete identity object with the trusted transferred identity before
the build. The source commit, tree, Git archive digest and lockfile digest must all
agree. Freeze a resource allocation covering the task directory, both printed
cache paths and the explicitly allocated task-local `ORT_CACHE_DIR`. This one
reviewed override selects cache storage only; it does not change native ONNX
selection or allow an ambient runtime library.
On the reported 128 GiB RAM/1.7 TiB free builder, allocate a **150 GiB aggregate
artifact/cache ceiling and 250 GiB free floor**, four Cargo jobs, an explicit
process-group RSS ceiling no larger than 96 GiB, and a 12-hour elapsed ceiling.
These are scheduler limits, not measured requirements. The 250 GiB floor
conservatively preserves the task's stop-below-250-GB constraint on every
involved filesystem. The builder's larger capacity does not lower that floor.

Use [`supervise_darwin_build.py`](supervise_darwin_build.py) for the exact build.
The coordinator supplies a private `recall-darwin-build-allocation/v1` file and
its independently reviewed SHA256. The file binds the original Mac START grant,
its time window, source identity, exact tool hashes, canonical source/task/cache/
SDK paths, the original shared resource ledger, and the shared Mac lane lock.
See [`darwin-build-allocation.example.json`](darwin-build-allocation.example.json)
for the inert schema example. It deliberately authorizes no execution.

The budget receipt must already exist, use the existing
`meerkat-recall-artifact-allocation/v1` format, and cover the task, both actual
repository cache paths, SDK/tools, lane and temporary/output filesystems. The
scheduler establishes that original ledger once, before allocated downloads and
build work, using the existing `ResourceBudget` helper. Preserve the same receipt
and filesystem set across retries and sequential related builds. The supervisor
refuses a missing receipt, changed device set, prior refusal, lock contention or relaxed limit;
it never resets consumed bytes. Its samples count all positive filesystem usage
increments, including foreign growth, and never refund deleted bytes.

The coordinator also creates the private shared lane-lock file once. Every job
using that Mac lane must honor it; this cooperative lock does not discover or
stop other tools. A nonempty marker means a prior build needs coordinator
reconciliation. The wrapper obtains an exclusive nonblocking lock and leaves a
persistent marker until it has observed cleanup. A process crash or uncertain
cleanup keeps the marker, and the receipt says `lane_release_ready: false`.
The wrapper does not send START/RELEASE messages or unlock the external scheduler.

The supervisor uses five-second filesystem/resource samples, fixed-size process
inventories, and a 256 MiB build-log ceiling. It performs no repeated cache-tree
walk. The allocation is sampled, not a kernel quota: disk/RSS may overshoot
between observations, and shared-filesystem churn can conservatively consume the
budget. Sampling uncertainty, expired allocation, low free space, excessive
consumption/RSS, log failure, cancellation or deadline stops the owned group.
The grant's original `started_at` and `expires_at` window must fit its elapsed
ceiling (at most 12 hours). Restarting the supervisor retains that original end
time. A 15-second cleanup reserve lies inside the grant; command cleanup and all
later retries share their original absolute deadlines. Expiry permits owned
signals and observations but only zero-time reap/drain waits. An incomplete
cleanup keeps the lane marker, including when another child's cleanup succeeds.
No cache is pruned. A bounded final cleanup inventories only that original group;
its unreaped session leader reserves the group identity until all signals finish.
If a log thread fails to start, cleanup still retains the child and closes the
idle pipe and log. Repeating cleanup after leader reaping rechecks capture
closure before marking the lane ready; it never signals that reaped identity.
The trusted build graph is expected not to detach processes; this is process
group supervision, not a sandbox against deliberately escaping build scripts.

The wrapper constructs its environment from explicit allocation fields, excluding
ambient credentials, proxies, Cargo/Rust/linker flags, ORT library overrides and
deployment-target overrides. Python and Rust stay task-local; the actual
repository cache remains in its dedicated allocation beneath the real user's
Library cache. Neither `HOME` nor global SDK/toolchain configuration is changed.
The task Cargo home must contain no credentials, config overrides or executable
`bin` directory that could shadow the pinned compiler. Logs, commands, source
identities, actual compiler/SDK/OS records, positive disk consumption and observed
RSS/free-space peaks are retained privately in a fresh attempt receipt directory.
The output directory must also be empty; retain earlier failed attempts separately
and reuse the same cache and original ledger after coordinator review.

Only after source freeze and the actual Mac START allocation:

```bash
"$recall_python" scripts/release/supervise_darwin_build.py \
  --allocation /absolute/reviewed/mac-build-allocation.json \
  --sha256 TRUSTED_FULL_64_HEX_ALLOCATION_DIGEST --plan
"$recall_python" scripts/release/supervise_darwin_build.py \
  --allocation /absolute/reviewed/mac-build-allocation.json \
  --sha256 TRUSTED_FULL_64_HEX_ALLOCATION_DIGEST
```

The first invocation only validates input paths, hashes and limits and prints an
inert plan. It starts no command and changes no lane or resource receipt. The
second invocation requires `execution_authorized: true`, the exact native host
and Python, and a current original START grant. It invokes only
`scripts/release/build-native aarch64-apple-darwin ALLOCATED_OUTPUT_DIRECTORY`
for the build, under the monitored lane. It never installs prerequisites.

This invokes one `nice -n 20` supported Cargo build, verifies unchanged source,
checks dynamic libraries, records `--help`/`--version`, and packages the exact
executable. Do not separately run a workspace build or broad test suite on this
host. A failure is a diagnostic receipt; retain its cache and log for the same
lane retry after review. Do not switch features to obtain an artifact.

## Verify and return

Select the sole returned `meerkat-recall-*-aarch64-apple-darwin.tar.gz`, compute its
SHA256, then use the trusted source verifier. The output directory must contain
only this build, making archive selection unambiguous.

```bash
recall_archive=("$recall_task"/output/meerkat-recall-*-aarch64-apple-darwin.tar.gz)
test "${#recall_archive[@]}" = 1
test -f "${recall_archive[0]}"
recall_archive_sha256="$(shasum -a 256 "${recall_archive[0]}" | awk '{print $1}')"
python3 scripts/release/artifact.py verify --archive "${recall_archive[0]}" \
  --sha256 "$recall_archive_sha256" > "$recall_task/receipts/archive-verify.json"
python3 scripts/release/artifact.py install --archive "${recall_archive[0]}" \
  --sha256 "$recall_archive_sha256" --destination "$recall_task/verified-install" \
  > "$recall_task/receipts/archive-install.json"
otool -l "$recall_task/verified-install/meerkat-recall" \
  > "$recall_task/receipts/macho-load-commands.txt"
shasum -a 256 "$recall_task/verified-install/meerkat-recall" \
  > "$recall_task/receipts/executable.sha256"
```

Also preserve the actual locked ONNX download and its static archive evidence.
The selected ort-sys 2.0.0-rc.12 ARM64 distribution is ONNX Runtime 1.24.2 at
`https://cdn.pyke.io/0/pyke:ort-rs/ms@1.24.2/aarch64-apple-darwin.tar.lzma2`, with
compressed-download SHA256
`612739f75438dc0a075461e1fb454226b4a1eb175e60a7271ba966bbbb972cd4`.
Locate the exact retained extraction used by this build from its verbose native
link output/cache receipts. Record every linked static archive's actual SHA256,
then run the builder's `otool -l` on those archives and retain the complete object
load-command output. If a retained archive or its minimum-version evidence is
unavailable, mark compatibility evidence unmet. Do not substitute a different
ONNX download or infer the minimum version from the distribution filename.

The Darwin acceptance wrapper requires a reviewed
`recall-darwin-compatibility/v1` receipt with `source_commit`, `archive_sha256`,
`binary_sha256`, `architecture: "arm64"`, `receiver_os_version: "26.5.2"`,
`binary_min_os`, `onnx_min_os`, `binary_load_commands_sha256`,
`onnx_archive_sha256`, and `onnx_load_commands_sha256`. `onnx_min_os` is the maximum
actual minimum across all linked ONNX object evidence. If several static archives
are linked, `onnx_archive_sha256` hashes the retained canonical JSON inventory of
their relative filenames and SHA256 values; retain that exact inventory and its
original files. The load-command digests hash the complete captured bytes, not a
summary. Both minima must be at or below 26.5 for this receiving allocation.
The final executable's system-library receipt and an actual clean-install run on
the receiving OS remain required even when these static checks agree.

Return the complete archive, independently communicated archive digest, source
identity, supervised build log, compiler/SDK/OS records, actual resource peaks and archive
verification receipt. Preserve Mach-O `LC_BUILD_VERSION`/minimum-OS evidence: a
newer builder SDK and ARM64 architecture do not establish compatibility with an
older recipient OS. Record the recipient's OS and run its separately allocated
clean-install acceptance. `otool -L` must show only permitted system libraries;
the packaged receipt already enforces that. Build success alone does not prove
provider, lifecycle or consumer acceptance. No launchd selection, production
service restart, paid provider call or deployment belongs to this build handoff.

The source for the separate native acceptance run is
[`darwin_offline_native.py`](../qualification/darwin_offline_native.py), with its
[review/allocation and isolation contract](../../tests/recall_preview_semantics/darwin_offline_README.md).
It reuses the installed case suite and requires actual Seatbelt denial controls;
it cannot claim a passed Darwin gate from source inspection or mocked controls.
