# Paired Recall remote Surreal server

This directory prepares the **upstream server package `surreal`**, distinct from the `surrealdb` SDK and `surrealdb-core` crates. It retains the upstream server's HTTP, WebSocket/RPC, SQL query, export/import and default storage features. It uses server version 2.6.5 at commit `8afd2ba0b6c0e232f19f42070e8811b323324c4c`, with the **same byte-exact reviewed Recall core, SDK and repaired SurrealKV 0.9.3** used by embedded/native Recall. Vanilla SurrealDB does not provide Recall's private reference-fence capability and cannot pass the paired readiness contract.

This recipe prepares and builds a source-pinned paired server. Qualification belongs to the exact version's artifact receipts; source preparation alone does not establish transport, restore, erasure or deployment acceptance. Ready-to-pull release images and authenticated installation use [qualified delivery](../../scripts/release/CONTAINER_DELIVERY.md) and [Compose](../../deploy/compose/README.md).

## Source and license identity

[source.lock.json](source.lock.json) pins the upstream commit/archive SHA-256, critical server files, Rust 1.94.0, and version-1 fixed capability vectors. The original [upstream server license](https://github.com/surrealdb/surrealdb/blob/8afd2ba0b6c0e232f19f42070e8811b323324c4c/LICENSE) is preserved byte-for-byte as [LICENSE.upstream](LICENSE.upstream), and remains with prepared sources and binaries. It is Business Source License 1.1, not Recall's MIT/Apache license; this recipe does not relicense it or claim unrestricted production use. Release review must preserve the actual upstream and dependency notices.

Preparation requires E's committed `vendor/surrealdb-core-2.6.5`, `vendor/surrealdb-2.6.5`, provenance manifests and static verifier, plus the reviewed SurrealKV 0.9.3 retention/lifetime repair and its original checksum manifest, patch and provenance document. Run the preparation recipe from that same reviewed checkout; its code and the vendor inputs are bound to one Recall commit. It refuses a different Recall commit, modified or untracked source input, an unrecognized vendor delta, an archive hash mismatch, archive traversal/special files, or an existing output directory. Source acquisition downloads no executable. No Cargo process, server, migration or provider call is part of preparation.

```sh
python3 packaging/recall-surreal/prepare.py \
  --recall-root "$PWD" --expected-recall-commit REVIEWED_FULL_RECALL_SHA \
  --archive /owned/source-cache/surreal-8afd2ba.tar.gz \
  --output /owned/this-worktree/paired-surreal-source --download
```

Omit `--download` for offline preparation using the exact cached archive. The core/SDK crate directories are replaced only inside a newly created, verified staging directory. The reviewed SurrealKV 0.9.3 fork is copied byte-exactly under `vendor/`, with its Apache-2.0 license, upstream checksum manifest and complete backport patch/document. Preparation verifies all 41 original files against that manifest plus the two pinned repaired files; the snapshot lifetime annotation is included. The root manifest forces registry edges to the copied core and SurrealKV and excludes the latter from workspace membership, preserving its dependency-only role.

The pinned server originally locks SurrealKV 0.9.1 and its `vart` 0.9.2 dependency. The explicit, validated lock transformation replaces these with Recall’s path SurrealKV 0.9.3 and its already reviewed registry `vart` 0.9.3 identity. It also adds the core’s direct edge to the already locked `hmac 0.12.1`. All unrelated resolved packages remain byte-equivalent as parsed lock entries, including the separate `vart 0.8.1` used by core. These are required fork-parity substitutions, not a general dependency refresh. Whether Cargo accepts this prepared lock must still be established by the first scheduled `--locked` build; preparation does not make that claim.

The output includes `RECALL_SOURCE_PROVENANCE.json` with all prepared file hashes, all three full vendor manifests, the Recall source SHA, upstream SHA/archive digest, lock identity and build metadata. The SDK/core overlays themselves are byte-exact: no second crypto parser or server-only security implementation is introduced. Preparation copies the complete reviewed core and SDK directories, including RPC dispatch and added files accepted by the vendor verifier. The server's HTTP and WebSocket handlers use those copied core protocol implementations; updating only a client SDK or reusing an older prepared tree does not update an already built server.

## Explicit scheduled build and container

The host entry point is `portable.py`. For a fresh build, select current host
capacity checks and a fresh receipt path:

```sh
export RECALL_BUILD_RESOURCE_MODE=current-capacity
export RECALL_ARTIFACT_BUDGET_FILE=/owned/release/paired-current-capacity.json
CARGO_BUILD_JOBS=16 python3 packaging/recall-surreal/portable.py \
  --recall-root "$PWD" --prepared /owned/release/paired-source \
  --bundle /owned/release/paired-bundle --tag recall-surreal:REVIEWED_SOURCE
```

This mode samples actual root free space (at least265GiB), each selected
source/cache/output filesystem (at least250GiB), and host MemAvailable (at
least120GiB); jobs are bounded to32. Shared host scheduling can choose fewer
jobs. Use a separate fresh receipt for Recall and the paired build: each keeps its
own immutable monitored path/device set. Receipts record actual samples; no obsolete cumulative allocation limits
apply and no previous receipt is reset or refunded. The paired compiler keeps
its independent250GiB free-space check. Sampling cannot prevent between-sample
growth. The launcher cancels only its own CLI process group on failure; verify
BuildKit activity has stopped before reusing that lane.

Omitting `RECALL_BUILD_RESOURCE_MODE` retains the historical `legacy-allocation`
mode and its original cumulative ledger/limits, for reproducibility of old
recipes. A legacy or refused record cannot be repurposed as current capacity.
A fresh current-capacity path is operational state, not another infrastructure
allocation or a requirement for an additional approval.

The shared `container-inputs.lock.json` pins the reviewed Linux/amd64 Bookworm
builder, runtime and Dockerfile frontend. Preparation copies that exact lock,
the release lane's unchanged `artifact.py` ELF inspector, and the internal build
recipe into the source manifest. The host launcher checks the clean reviewed
checkout, prepared commit, source/binary/ELF joins and immutable image ID. In current-capacity mode the paired cache binds compiler, target, profile,
package/features, pinned container/native inputs and Cargo wrapper compatibility.
Source-only changes and checkout paths do not force a cold cache; Cargo still
fingerprints source changes. Legacy mode retains its old source/path cache keys.

Inside the pinned builder, `build.py` uses the copied `scripts/repo-cargo` wrapper,
`chrt --idle 0 ionice -c3`, upstream defaults and
`--locked --release -p surreal --bin surreal`. The default is eight jobs, with a
maximum of32 in current-capacity mode (16 in legacy mode); incremental compilation is off and
both worker limits are eight. No default backend/server feature is removed.
The internal disk monitor is additional protection, not another allocation.
Use the host launcher so actual disk and memory remain monitored. Coordinate
heavy work on shared hosts; no separate historical slot/ACK gate is introduced.

The build records the actual verbose compiler identity, host libc, installed
builder packages and binary ELF imports. It refuses a host ABI newer than Debian
12. The shared ELF inspector uses `readelf` without executing the paired server
and rejects unknown dependencies or imported ABI versions beyond its reviewed
Debian 12 limits. This paired-server recipe targets Linux x86-64; a Darwin paired
server is not qualified. HomeCore's separate Recall native artifact recipe does
not require building this server locally.

After verifying the bundle, the same host launcher builds the thin runtime image
from the exact locked runtime input. It installs the declared Debian 12 runtime
libraries and records actual `ldd` output and package versions. Package repository
resolution is observed, not claimed bit-for-bit immutable. The host receipt
`CONTAINER_BUILD_PROVENANCE.json` records both commands, BuildKit metadata, source
SHA, binary SHA, immutable local image ID, available repository digests, exact
cache identity and shared allocation receipt. A successful build is explicitly
`built_unqualified`: it does not establish service, transport, restore or release
acceptance.

The bundle retains the upstream/core/SDK/SurrealKV licenses, fork patch/provenance,
source/build receipts and image-input lock. The upstream version incorporates
`+recall.reference-v1.<Recall-SHA-prefix>`. The container runs as UID/GID 65532 and
defaults to `--help`, with an owned writable `/data` directory for new volumes.
An operator explicitly supplies `start`, binding and persistent store location.
Supply `SURREAL_USER` and `SURREAL_PASS_FILE` through the image secret adapter, never image
layers, checked-in files or receipts. TLS and listener exposure remain explicit.

Resource checks are sampled before/during/after both builds and bundle export.
They cannot prevent between-sample growth. On refusal the launcher cancels only
its owned Docker CLI process group; the scheduler must confirm the associated
builder has stopped before allocating another job. No unrelated cache, image or
process is deleted. Pure tests below do not build or start either image. Exact executed artifact
receipts establish each release's actual qualification scope.

## Capability and readiness

Once a separately authorized server is running, `verify.py` can perform a read-only `/version` and `/sql` probe using fixed **nonsecret** HMAC vectors. Set `RECALL_SURREAL_PROBE_USER`/`RECALL_SURREAL_PROBE_PASSWORD` through the deployment secret mechanism and supply explicit namespace/database and exact build metadata:

```sh
python3 packaging/recall-surreal/verify.py \
  --endpoint https://PAIRED_SERVER --namespace QUALIFICATION_NAMESPACE \
  --database QUALIFICATION_DATABASE \
  --expected-build-metadata recall.reference-v1.REVIEWED_SHA_PREFIX
```

The probe checks native function version 1 and the distinct String("42")/Number(42) tokens, uses bounded responses and TLS verification, and refuses redirects instead of forwarding authentication. It never reads the original store's private key, creates schema, imports a backup, or accepts a version string alone as capability proof. Its success receipt is only version and pure-function capability evidence.

Recall's F-owned client constructor separately supplies trusted private context and performs its actual capability/read-only guard checks before Ready. **Serve verifies schema; it does not apply migration 039 or other schema.** Explicit `migrate` initializes the guard. Privileged restore reinstalls current and historical extension guards under the exclusive restore barrier and replays independently durable private membership before Ready; other missing migrations still block. An ordinary server health response or this pure-function probe does not certify those steps. Keep the original private journal/key/membership custody separate from database snapshot retention.

Use normal bounded operator logging. Do not enable request/auth/body/binding dumps in a qualification or deployed server. E/F own narrow redaction of trusted private bindings in the pinned Context/Query debug paths; that source hardening and sentinel regression must join the paired source before qualification. Raw database administrators remain trusted: this does not claim to hide intentionally selected query variables from an administrator.

## Required qualification receipts

The actual paired artifact must still demonstrate authenticated HTTP and WebSocket/RPC connection/query behavior, both fixed native vectors over actual transports, guarded ordinary create/update rollback, physical source erasure, separately connected concurrent writers, retained export/import through the authorized restore path, old-schema/pre-source backup replay, restart/recovery and refusal before Ready with missing/mismatched private custody. Preserve package SHA, upstream/Recall/vendor provenance, binary/image digests, toolchain/lock/features, version/capability outputs, exact commands and result status without secrets. Real remote restore/write checks belong to D/F/A/E's oracles, not this nonmutating probe.

Offline tests (no compiler, service or network):

```sh
python3 -m unittest discover -s packaging/recall-surreal/tests -v
```

## Retained development Compose examples

The repository's two development Compose files require an explicit
`RECALL_SURREAL_IMAGE` naming the locally built/loaded image from this recipe.
They do not fall back to vanilla SurrealDB or pull a database image. The paired
binary path is `/usr/local/bin/recall-surreal`, including its `is-ready` health
command. See [DEV.md](../../DEV.md) for explicit migration, persistent original
journal/backups, loopback ports and the distinction between liveness and
Recall's authenticated readiness. These examples do not establish artifact
qualification or start any service as part of source preparation.
