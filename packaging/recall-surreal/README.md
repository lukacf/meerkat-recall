# Paired Recall remote Surreal server

This directory prepares the **upstream server package `surreal`**, distinct from the `surrealdb` SDK and `surrealdb-core` crates. It retains the upstream server's HTTP, WebSocket/RPC, SQL query, export/import and default storage features. It uses server version 2.6.5 at commit `8afd2ba0b6c0e232f19f42070e8811b323324c4c`, with the **same byte-exact reviewed Recall core, SDK and repaired SurrealKV 0.9.3** used by embedded/native Recall. Vanilla SurrealDB does not provide Recall's private reference-fence capability and cannot pass the paired readiness contract.

Status: source preparation and offline test source. No paired binary/container has been built or executed by this lane. No transport, restore, concurrent erasure, native platform, release or deployment acceptance is claimed. Heavy builds and running servers remain separately scheduled qualification work.

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

The centrally scheduled host entry point is `portable.py`. It uses the same
reviewed `scripts/release/container_artifact.py::ResourceBudget` as Recall's
container build. Both sequential builds **must use the exact same absolute**
`RECALL_ARTIFACT_BUDGET_FILE` and filesystem set. There is no automatic second
allocation, reset or cache pruning. The shared receipt is locked, atomically
persisted and synchronized; positive observed disk increments accumulate without
refund after deletion. It applies the hard 250 GiB floor and a 150 GiB default
aggregate allocation across source, Docker, output and temporary filesystems;
an explicit central allocation may raise that shared ceiling up to 300 GiB.
The paired compiler retains its separate 150 GiB maximum growth ceiling. The
launcher passes the smaller of the shared allowance and that inner ceiling,
while all source, Docker and output growth still counts against the original
shared ledger. A larger shared allowance neither resets its counters nor raises
the inner compiler limit.

After source review and central allocation, with no load-sensitive Meerkat gate:

```sh
export RECALL_ARTIFACT_BUDGET_FILE=/owned/shared/recall-artifact-allocation.json
CARGO_BUILD_JOBS=8 python3 packaging/recall-surreal/portable.py \
  --recall-root "$PWD" \
  --prepared /owned/this-worktree/paired-surreal-source \
  --bundle /owned/this-worktree/paired-surreal-bundle \
  --tag recall-surreal:REVIEWED_SOURCE
```

The shared `container-inputs.lock.json` pins the reviewed Linux/amd64 Bookworm
builder, runtime and Dockerfile frontend. Preparation copies that exact lock,
the release lane's unchanged `artifact.py` ELF inspector, and the internal build
recipe into the source manifest. The host launcher checks the clean reviewed
checkout, prepared commit, source/binary/ELF joins and immutable image ID. The
paired cache binds the full prepared provenance digest, canonical prepared path,
compiler, target, package and feature graph. Retries reuse that exact cache;
separate prepared contexts do not share targets even at the same commit.

Inside the pinned builder, `build.py` uses the copied `scripts/repo-cargo` wrapper,
`chrt --idle 0 ionice -c3`, upstream defaults and
`--locked --release -p surreal --bin surreal`. The default is eight jobs, with a
hard maximum of 16 under central scheduling; incremental compilation is off and
both worker limits are eight. No default backend/server feature is removed.
The internal disk monitor is additional protection, not another allocation.
Do not invoke the internal build or either Dockerfile directly to bypass the
host allocation monitor. No artifact build is authorized during another
load-sensitive gate without the scheduler's agreement.

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
Supply `SURREAL_USER`/`SURREAL_PASS` through the secret mechanism, never image
layers, checked-in files or receipts. TLS and listener exposure remain explicit.

Resource checks are sampled before/during/after both builds and bundle export.
They cannot prevent between-sample growth. On refusal the launcher cancels only
its owned Docker CLI process group; the scheduler must confirm the associated
builder has stopped before allocating another job. No unrelated cache, image or
process is deleted. All instructions and tests here are source-only; this lane
has not built or started either image.

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
