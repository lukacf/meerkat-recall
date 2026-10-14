# Native artifact recipe v1

Run from a clean reviewed checkout on the native target host with Rust 1.94,
Python 3.12+, native linker dependencies and sufficient local build disk. The default floor is
32 GiB; set `RECALL_BUILD_MIN_FREE_GIB` to the allocated host requirement (250 on
the shared implementation host). This is a preflight floor, not a measured peak
disk guarantee. Coordinate build slots;
the recipe defaults to 8 Cargo jobs on Linux and 4 on Darwin (at most 16),
with idle scheduling on Linux and `nice` on Darwin. It never
publishes, installs, deploys or calls a model provider. A cold build downloads
locked Cargo dependencies and the target-specific checksum-pinned ONNX Runtime
distribution described below; model weights are provisioned separately.

```sh
scripts/release/build-native aarch64-apple-darwin /absolute/artifacts
# Or, on native Linux x86-64:
scripts/release/build-native x86_64-unknown-linux-gnu /absolute/artifacts
```

The same source identity is checked before and after compilation. The build
receipt binds the source commit/tree, `git archive --format=tar HEAD` SHA256,
Cargo.lock digest, target, compiler, profile, features and executable digest.
Both native and container candidates use default features plus
`--features rocksdb,fastembed`: the ordinary production binary supports the
local 384-dimensional FastEmbed and configured OpenAI embedding paths. There is
no separate qualification binary. `otool -L` or `ldd` output is bound to the
executable digest; packaging refuses Homebrew, unbundled ONNX and unsupported
runtime library dependencies. The supported Linux runtime is Debian 12 or a
compatible system with the recorded system libraries.

Linux uses the CPU static ONNX Runtime 1.24.2 / API 24 distribution recorded in
`linux-static-ort.lock.json`, published by `csukuangfj/onnxruntime-libs` from its
[recorded manylinux 2014 / GCC 11 build](https://github.com/csukuangfj/onnxruntime-build/actions/runs/22630506217).
The locked `ort-sys` GNU archive references glibc 2.38 C23 functions and cannot
link on the supported Debian 12 builder. The release selects this compatible
static input explicitly; it does not change Cargo.lock, Rust 1.94, default
features plus `rocksdb,fastembed`, or the GLIBC 2.36 / GLIBCXX 3.4.30 /
CXXABI 1.3.13 final ELF limits. Darwin still uses the locked `ort-sys` distribution.

`linux_static_ort.py` verifies the compressed ZIP and every extracted member,
requires the API 24 header and a regular static archive, rejects aliases and
unexpected archive entries, and prepares an immutable digest-addressed cache
under the current Cargo home. A cache hit is reverified; corruption is an error.
The one HTTPS download has a 30-second socket timeout, a 120-second transfer
check and an exact size bound, with no retry or fallback. The recipe refuses
ambient ORT path/link overrides, then sets its verified `ORT_LIB_PATH` internally
with dynamic linking disabled. The digest is part of that path; `ort-sys` tracks
changes to this environment variable, invalidating the affected native build
while unrelated Cargo outputs may remain warm. The completed-build receipt
binds the selected dependency fingerprints, static link directive, exact archive
and header bytes, source lock, publisher build coordinate and original registry
crate. It labels this distribution separately from `ort-sys`'s unused GNU table.
The Linux final-binary build script also links the native compiler's real
`libgcc.a`, needed by this distribution's CPU feature initializer. It asks `cc`
for that archive's absolute path, tracks changes to the archive, and passes it
only to the final GNU Linux FastEmbed binary link. The receipt binds that
selected build script's output and the actual archive bytes. There is no symbol
shim, global Rust flag, or replacement C/C++ standard library. Linux packages
include ONNX Runtime's license and third-party notices.

This is a pinned third-party static distribution, not a locally reproduced
ONNX Runtime build. Its publisher platform and static-symbol inspection are
input evidence; successful final linking, unchanged ELF checks and actual
FastEmbed inference remain separate qualification requirements. Do not infer
runtime acceptance or change the minimum supported system from the filename.

The `meerkat-recall-native/v1` manifest additionally hashes every bundled file
and records the compatibility symlink. Packaging includes extraction skills,
service templates, license texts and the six patched vendored dependency trees.
Meerkat 0.9.0 supplies the provider HTTP defenses upstream;
`meerkat-provider-upstream.json` records the registry source/checksums that replace
the historical HTTP backports, with exact published license texts retained under
`licenses/meerkat-0.9.0` (container: `/usr/share/licenses/meerkat-recall/meerkat-0.9.0`). Native archives
and container images carry the same patched source inventory. This includes the
pinned SurrealDB core and SDK 2.6.5, their complete Business Source License 1.1
texts, upstream checksums and `RECALL_SURREALDB_PATCH.md`. These component
licenses are recorded separately from Recall's project license. The source
receipt binds the exact core/SDK trees and declares reference-token capability
version 1; packaging verifies the bundled sources against that receipt. Build
recipes run the offline vendor audit before compiling.
The archive SHA256 is external to the archive. Obtain it from a trusted release
record. Provenance records are descriptive metadata, not signed attestations.

Verify or install without Cargo, running from a trusted copy of this script:

```sh
python3 scripts/release/artifact.py verify --archive candidate.tar.gz --sha256 TRUSTED_SHA256
python3 scripts/release/artifact.py install --archive candidate.tar.gz --sha256 TRUSTED_SHA256 \
  --destination /absolute/releases/recall-VERSION-DIGEST
```

Installation requires a new destination and retains the entire archive tree.
Select its executable through the service manager after separate qualification;
installation does not start the service or modify a running installation.
Verification checks archive paths/types, the complete manifest and every file
before publishing the directory. It does not execute the candidate binary.

Offline recipe tests: `python3 -m unittest discover -s scripts/release -p 'test_*.py'`.
Final qualification separately needs the actual native Darwin ARM64 and Linux
hosts, authenticated HTTP/MCP fixtures, persistence/restart/restore, shutdown,
provider and consumer acceptance. Missing hosts or providers are prerequisites,
not passes. Never transfer acceptance to a rebuilt binary based on version alone.

## Container recipe

```sh
export RECALL_ARTIFACT_BUDGET_FILE=/absolute/artifacts/shared-allocation.json
scripts/release/build-container recall-candidate:SOURCE /absolute/artifacts
scripts/release/render-container --image registry.example/recall@sha256:DIGEST \
  --output /absolute/artifacts/recall.yaml
```

The build context is the committed Git archive. The local image receipt records
source identity, local image ID, platform and any already assigned repository
digests. A local image ID is not a registry manifest digest. The script neither
pushes an image nor applies the rendered StatefulSet. The image carries a
`meerkat-recall-container/v1` source/build manifest and patched dependency trees.
Direct development `docker compose build` labels provenance as
`unversioned-development`; it cannot substitute for this exact-source recipe.

The container recipe performs **one Cargo build** and produces both the local
Linux/amd64 image and the native Linux archive from the exact same executable.
It uses `docker buildx`, pinned Bookworm builder/runtime manifests and a pinned
Dockerfile frontend recorded in `container-inputs.lock.json`. Native dependencies
are installed from Debian's repositories; every installed builder/runtime package
version is recorded. Those recorded versions do not claim that mutable package
repositories produce a bit-for-bit identical image on a later date.

The build keeps a dedicated Bookworm Rust cache with exclusive cache-mount access,
bound to the reviewed source identity, checkout path, compiler, target and feature
graph. Retries of that same build reuse its cache; another checkout or source
revision gets a separate cache. Do not seed it with Ubuntu target objects. Build
jobs are bounded to16 (default8), with `TLC_WORKERS=8` and `BINARYEN_CORES=8`.

The scheduler must allocate one Recall Cargo slot and set the same absolute
`RECALL_ARTIFACT_BUDGET_FILE` for the sequential Recall and paired-server builds.
The persistent allocation receipt monitors source, Docker, output and temporary
filesystems, deduplicating shared devices. Every positive observed usage increment
consumes the aggregate `RECALL_ARTIFACT_MAX_GIB` allocation (default150; cannot
exceed1280). An explicit value above150 requires the host scheduler's approval.
Increasing this setting never raises an existing ledger's limit: that requires
a separate coordinated transition while every prior ledger reader is stopped.
The transition must retain the original baseline and consumption, and must not
lower the free-space floor. The approved600 GiB transition also raises the stored
floor to at least300,000,000,000 bytes (300 GB decimal). Admission, readiness and
every recurring sample must enforce the greater of that floor and the original
250 GiB floor; any stricter stored floor remains authoritative. Future consumers
must explicitly request the approved allocation; the
default remains150. Later deletion does not refund it; unrelated filesystem growth also
counts. The wrapper checks every two seconds during the build and refuses at the
allocation limit or below `RECALL_BUILD_MIN_FREE_GIB` (default250; cannot be lowered).
Sampling cannot attribute bytes to processes or prevent growth between samples.
An allocation refusal, changed filesystem set or changed capacity requires scheduler
review. A larger ceiling does not clear an existing refusal: ordinary consumers
continue to reject it. The separately reviewed, expiring 800 GiB transition may
clear only the original cap-exhaustion refusal under explicit coordinator and
host authority, with all prior readers drained and positive intervening growth
checked under the original lock. It retains original counters and samples in immutable history, charges each
positive observed delta before advancing the sample, and never refunds a later
decrease; a floor refusal, unknown
cause or exhausted new ceiling remains refused. This transition is not performed
by the build scripts. The script never resets the allocation or prunes caches. Build cancellation
still needs the scheduler to confirm the builder has stopped before another Cargo job.

The native archive adapter checks the immutable local image ID and platform,
source labels, source/lock digests, pinned image inputs, compiler, production
features, exact binary digest, vendor receipt, actual ELF imports and the installed
runtime library/package inventory. `readelf` inspections reject requirements newer
than Debian12's GLIBC2.36, GLIBCXX3.4.30 and CXXABI1.3.13 baseline, as well as unknown
dependencies. The executable is copied from a stopped container and is neither
recompiled nor executed by extraction. The stopped container is removed afterward.
Runtime `ldd` evidence is collected in the target image during its build.

To package an already built image again, use its immutable local ID and a new
output directory from the same clean reviewed checkout:

```sh
python3 scripts/release/container_artifact.py package \
  --image-id sha256:ACTUAL_LOCAL_IMAGE_ID --expected-source REVIEWED_FULL_SOURCE_SHA \
  --output-dir /absolute/new-artifacts
```

A local image ID is not a registry digest. Review updated manifest/config/platform
metadata before changing the input lock. The direct `build-native` path remains
available, including Darwin; Linux builds on newer hosts must pass the same ELF
baseline check and cannot claim Debian12 support from a library name alone.
Installed native/container acceptance remains separate from build and packaging.


## Local model assets

The archive/image includes the FastEmbed runtime, not downloaded model weights.
The governed FastEmbed path uses the existing provider connection and profile:
`endpoint` is an absolute `file:///.../model-assets` root and `model_version` is
`sha256:` followed by the immutable manifest digest. It reads
`model-assets/DIGEST/manifest.json`, verifies every ONNX and tokenizer byte, and
initializes only those verified bytes without Hugging Face downloads. The manifest
also binds model, dimensions, preprocessing and external initializer files.
Unknown/missing/mismatched versions fail before inference. Ordinary readiness
can verify available bytes; successful inference still needs model acceptance.

Provision already selected model files offline, then review the output and
approve its exact endpoint/version in the ordinary space processing configuration:

```sh
scripts/release/provision-fastembed --root /absolute/state/model-assets \
  --model bge-small-en-v1.5 --onnx /absolute/reviewed/onnx/model.onnx \
  --tokenizer-dir /absolute/reviewed
```

The tokenizer directory must contain `tokenizer.json`, `config.json`,
`special_tokens_map.json` and `tokenizer_config.json`. For a model with external
initializers, repeat `--external-initializer ONNX_FILENAME=/absolute/local/file`.
The helper copies files into a new content-addressed version directory and hashes
the copied bytes. It never downloads, starts a model or changes provider approvals.
Select and retain trusted model origin/revision/license evidence separately;
a checksum binds selected bytes but cannot establish their origin by itself.
The local `bge-small-en-v1.5` name denotes the BGE model with CLS pooling. The
locked FastEmbed catalog points to an ONNX conversion at
`Xenova/bge-small-en-v1.5`; approving the upstream BAAI model name alone does not
select or verify a particular ONNX conversion. Freeze that conversion's origin,
revision and file hashes before recording semantic qualification.
The service account needs read access to its approved asset root. Transfer the
complete immutable version directory alongside the binary when provisioning a Mac.

The service templates keep `FASTEMBED_CACHE_DIR` in private persistent state for
retained ungoverned local tools. Governed provider dispatch does not consult that
cache or `HF_HOME` and never falls back to mutable upstream model names.

Capture provisioned model assets before and after separately authorized
exact-artifact acceptance, writing receipts outside the asset root:

```sh
python3 scripts/release/artifact.py model-cache-receipt \
  --cache /absolute/state/model-assets --binary-sha256 EXECUTABLE_SHA256 \
  > /absolute/evidence/model-assets-before.json
```

This receipt hashes every file and resolved internal symlink, binds the executable
digest, and rejects links outside the root. It performs no download or inference.
The provider acceptance receipt must identify the exact approved profile version.

## Apple Silicon handoff

Use [the allocated builder handoff](DARWIN_BUILDER.md) for exact source transfer,
task-local tools, cache identity, lane scheduling and receipt return.

Transfer the complete `aarch64-apple-darwin` archive and a trusted archive digest,
then run offline `verify` and install into a fresh directory. A prebuilt archive
needs extraction/model-cache space and no Rust toolchain or build target cache.
It still requires acceptance on the recipient Mac before selecting it in launchd.

For a local native build, use the exact reviewed source commit, Rust 1.94.0,
Python 3.12+, and Apple command-line tools with a working SDK, C/C++ compiler,
linker and `libclang` for RocksDB's bindgen step. Full Xcode is not inherently
required; verify the selected command-line tools can supply these inputs. The
supported package is `meerkat-recall-bin`, with default features retained and
`rocksdb,fastembed` enabled. Avoid workspace/all-target/all-feature builds when
producing this artifact.

The pinned graph does not require a `protoc` or audio-toolchain installation.
FastEmbed uses ort-sys's checksum-pinned native ARM64 ONNX Runtime distribution;
it does not compile ONNX Runtime or protobuf in this recipe. `LZMA_API_STATIC=1`
builds vendored liblzma C sources with the C compiler. Package-config probes in
other bundled compression libraries have source fallbacks; neither a missing
`pkg-config` nor `cmake` alone establishes a missing requirement for this selected
graph. Recheck these statements when updating the lockfile or enabled features.
Build downloads still require network access or exact verified warm caches.

The reviewed artifact recipe uses `--profile deploy`, preserving all supported
features while disabling LTO and reducing optimization of the generated Meerkat
schema crate. Normal `--release` enables full LTO and one codegen unit; its peak
memory is not equivalent, and its receipt is not accepted by this packager. The
native recipe defaults to four jobs and requires at least 32 GiB free before
building. That check does not establish a peak RAM or disk bound. For a live host
with about 6 GiB available RAM, 29 GiB free disk and a target budget below 15 GiB,
prefer qualifying a prebuilt native archive. The build currently refuses that
host's disk preflight; do not bypass it based on an unmeasured size estimate.

If a constrained local build is separately allocated, start with one Cargo job
and monitor actual process memory, free disk, and both the target and Cargo/ONNX
caches. Even one compiler process may exceed the available memory. The repository
wrapper places Darwin caches under `~/Library/Caches/rust-workspaces`; inspect
its `--print-env` output, since it overrides an ambient `CARGO_TARGET_DIR`. Keep
the same checkout, lane and feature graph for retries, and never prune another
process's cache. The shared Linux implementation host's 250 GiB floor is a host
allocation rule, not a macOS installation requirement.


## Compatible remote database server

Remote mode requires a server built from the same reviewed SurrealDB 2.6.5 core
patch. The paired source preparation and candidate server recipe lives in
`packaging/recall-surreal/README.md` in the exact committed source checkout.
Native archives and Recall container images include the patched core/SDK source
and their license texts; they do not include or start a separate database server.

The server must pass both `recall::reference_token_version()` and the independent
fixed-vector probe described in `third-party/RECALL_SURREALDB_PATCH.md` before
Recall admits storage. An unmodified upstream 2.6.5 server lacks this capability.
Keep the paired server source/build/image and capability-probe receipts with the
Recall artifact receipt. A source fingerprint describes build inputs; it does
not substitute for probing the running remote server. The runtime's journal,
erasure replay, migration and readiness checks remain part of acceptance.

The scheduled container builder may pass the keyword-only `cache_id` to
`container_artifact.build_image` to reuse one explicitly reviewed warm Recall
BuildKit cache across a recipe correction. The default remains derived from the
current checkout/source/feature graph. Explicit values must have the exact
`recall-bookworm-rust194-` plus 64 lowercase hexadecimal digest shape. Both the
actual Docker build argument and `container-build-metadata.json` retain that
selection. Current source identity, archive identity and dependency/native-input
invalidation checks remain in force; cache reuse is not artifact acceptance.
