# Isolated gateway runtime packaging

`prepare_gateway_runtime.py` creates the exact regular-file runtime consumed by
G's existing `tests/recall_preview_semantics/gateway/launcher.py`. It does not
build, download, install packages, execute a candidate, start services, run the
gateway, discover credentials or contact a provider. Output is explicitly
`packaged_unqualified`. Packaging does not establish installed acceptance.

Use a clean, reviewed integrated checkout containing the release helpers, G's
gateway and the reviewed harness files. Host prerequisites are Linux, Python
3.11+, `/usr/bin/git`, `/usr/bin/dpkg-deb`, and `/usr/bin/readelf`. These are
inspection tools. No package maintainer scripts or runtime Python are executed.
The candidate interpreter comes from explicit reviewed Debian 12 amd64/all `.deb`
inputs, never from the Ubuntu host interpreter or its libraries.

`runtime_package_inputs.example.json` is an unfilled schema example. Replace
EVERY placeholder with approved existing paths, exact versions, byte lengths and
SHA256 values. Add every necessary dependency package; the example is not a
working package lock or permission to acquire packages. Required inputs are:

- An already verified, unpacked native Recall artifact, its exact
  `provenance.json` hash, and original source commit. All manifest file hashes,
  build capabilities, vendored reference sources, licenses and actual ELF
  dependency metadata are checked against the release helper's authority.
- The corresponding already built paired `recall-surreal` bundle, exact build
  and source provenance hashes, reviewed Bookworm builder inputs, actual binary
  and ELF identity, and upstream/KV source/license records. Only this explicit
  bundle allowlist is copied; adjacent files are not runtime inputs.
- Explicit Debian package paths, SHA256, lengths, Package, Version and Architecture.
  Supply `python3.11-minimal`, its standard library packages and all transitive
  ELF libraries. Select runtime paths explicitly. Every selected symlink or
  hardlink resolves only through supplied package entries, then becomes ordinary
  copied bytes. Missing targets, escaping or cyclic aliases, special files,
  set-id payloads and conflicting package paths fail closed. Every package's
  copyright record is included. Packages are not installed and no post-install
  generated CA bundle is invented; the isolated child uses the loopback gateway.
- Exact reviewed harness files with source-relative path, hash and byte length.
  Preserve `/opt/recall/scripts/qualification/` and
  `/opt/recall/tests/{recall_preview,recall_preview_semantics}/`. The exact
  `scripts/release/artifact.py` dependency is also admitted. Nothing is copied
  from the operator's home, ambient credentials, a Python virtualenv or host libs.
Launch inputs are a **later phase**. Runtime input format
`recall-gateway-runtime-inputs/v2` has no launch-spec field. First seal the runtime
and its hash; then create static qualification plans referencing that runtime,
independently review them, seal their input-tree inventory, and construct the
outer launch specification. The static plans contain neither their own review
reference nor the later launch-spec hash. Only the outer approved launcher binds
all these stages. In-runtime provenance contains no future launch-spec/input-tree
hash. This order prevents circular hashes; do not guess a future digest.

The later `recall-gateway-launch/v1` specification uses absolute
`/usr/bin/python3 -I /opt/recall/scripts/qualification/SCRIPT.py ...`, the actual G
binding list, and optional `inputs: {directory, inventory_sha256}`. G snapshots
that original host input directory under its exact bounds and mounts it read-only
at `/inputs`. These input bytes are not packaged into the runtime. No shell,
PYTHONPATH, JWT_SECRET, SKIP_AUTH or provider key is admitted in launch environment.
Isolated Python scripts load reviewed sibling modules explicitly. G injects
synthetic local authentication and credential-free binding metadata only after
separate approval. Include the actual reviewed `gateway/launch_context.py` and
all its transitive sources in the packaged harness.

The packager verifies every copied ELF's amd64 class, interpreter and NEEDED
closure against runtime files only, rejects ambiguous library bytes, RPATH and
RUNPATH, and refuses glibc versions newer than Debian 12's 2.36 baseline. It
checks the Python 3.11 standard library and an explicit module contract. Debian's
minimal interpreter builds `_socket`, `select`, `_random` and `_struct` into
the executable. Each requires its defined global `PyInit_*` function in the
packaged interpreter's dynamic symbol table. `_ssl` and `_hashlib` require actual
inspected ELF files at their Python 3.11 amd64 `lib-dynload` import paths. An
undefined symbol, another symbol type, unrelated file or missing extension cannot
substitute for these checks. The receipt records both module categories.
These static checks do not prove that imports or candidate processes work.
Actual isolated interpreter/service acceptance is a separate qualification.

The category evidence is Debian source `python3.11` version
`3.11.2-6+deb12u8`: `debian/PVER-minimal.README.Debian.in` marks the four built-ins
`extension` and the two loadable modules `extensionx`; `debian/rules`'s
`__post_configure` selects only exact `extension` entries into `Modules/Setup.local`
before generating `Modules/config.c`. This is not a general exemption for missing
`.so` files. The recipe can be read at
<https://sources.debian.org/src/python3.11/3.11.2-6%2Bdeb12u8/debian/>.
The reviewed metadata custody is Debian's signed Bookworm InRelease through the
SHA256 `Sources.xz` entry to `python3.11_3.11.2-6+deb12u8.debian.tar.xz` and its
exact two members:

| Metadata | SHA256 |
| --- | --- |
| Bookworm Sources.xz | `92d11a035571df011f06f28c61cb9825130ef1d92abb2f2e0525e8674dc44af8` |
| Debian source recipe archive | `ece2d63c70ac0b7a401fbe6b51b0103be997c294a01a3865549be1862794ba97` |
| Minimal module list | `f1db7d833fa7ce7dc66e37ebd5fb3826c1a5d5e225dadeb392b2d26338b1b64d` |
| debian/rules | `4918801d0330e10868e2323283198743f6e00bd67dc63ef102f6d71a8efdac07` |

These metadata hashes describe the reviewed source evidence; exact binary package
identity and every packaged ELF remain independently checked by the packager.

The output always includes both real binaries at `/opt/recall/bin/elephant` and
`/opt/recall/bin/recall-surreal`. Native skills and documentation remain at
`/opt/recall/share/native/...`; use
`PIPELINE_SKILLS_DIR=/opt/recall/share/native/skills` when approved by the actual
launcher. Keep both native and remote-backend services inside the same gateway
namespace. A host service endpoint or omitting the isolated namespace is not a
supported fallback. Required `/proc`, `/dev`, `/tmp`, `/home` and `/work` mount
points are present, along with `/inputs` and empty regular mount targets
`/bridge.py`, `/launch_context.py`, `/launch-anchor.json`, `/gateway.sock`, before
tree hashing. G supplies the actual private/read-only mounts; it need not create
destinations inside the sealed root.

Before packaging, the scheduler must provide the SAME absolute
`RECALL_ARTIFACT_BUDGET_FILE` used by Recall artifact builders and the original
`RECALL_ARTIFACT_DOCKER_ROOT` filesystem path. This does not contact Docker. The
shared `ResourceBudget` preserves the 250 GiB free-space floor and cumulative
150 GiB allocation, with no refund for deletion and no reset. The exact original
filesystem set must agree. The original receipt must already exist as a canonical
regular private file: this consumer never starts a new budget allocation. The package additionally caps the virtual input pool
at 4 GiB, output at 2 GiB, total runtime entries below 100,000, each `.deb` at
512 MiB, package count at 32 and deadline at 600 seconds. Default deadline is
300 seconds. Source archive provenance is streamed with a 2 GiB bound; no second
source archive is stored. Disk checkpoints occur during byte copying. Inspection
children run with a cleared environment and are killed on their deadline. The
leader stays unreaped until the timeout thread has joined and its owned process
group has been cleaned up, preventing a late signal after PID reuse. Copying uses
the frozen manifest hashes rather than a digest observed after verification; both
copied executable hashes must equal their build receipts. The checkout must
remain at the same clean commit through publication.

Once this source and every input are independently reviewed and the scheduler
has allocated the work, the concrete packaging command is:

```sh
python3 scripts/qualification/prepare_gateway_runtime.py \
  --recall-root /absolute/clean/reviewed/checkout \
  --inputs /absolute/reviewed/runtime-inputs.json \
  --output /absolute/owned/runtime-root --deadline-seconds 300
```

Use a new output path in an operator-controlled directory. All directories are
sealed 0555 and files 0444 or 0555. The package invokes G's ACTUAL `tree_hash`,
records G's actual Python-source hash and original artifact hashes, and writes a
private adjacent `runtime-root.receipt.json`. It does not record a future launch
identity inside the sealed runtime. The in-tree
`/opt/recall/share/runtime-package.json` contains full input/file/ELF provenance.
If receipt publication fails after the runtime rename, preserve the runtime for
inspection; the packager does not silently delete or overwrite a published tree.
A partial private staging tree is cleaned up only by its creating invocation.

After the actual static plans/review/input inventory and launch specification
exist, the same tool has a separate static validation mode.

Static admission reads the existing private allocation and honors its retained
`minimum_free_bytes`, with the original 250 GiB lower bound. A coordinated
300,000,000,000-byte floor or any stricter retained floor therefore applies to
the source, sealed runtime and output filesystems before helpers are loaded or
a receipt is written. Missing, malformed, refused, exhausted or mismatched
filesystem allocations refuse. This read does not create an observer, initialize
an allocation, change counters or lower its floor; the generic allocation
defaults and the original runtime/isolation requirements remain unchanged.

```sh
python3 scripts/qualification/prepare_gateway_runtime.py \
  --recall-root /absolute/clean/reviewed/checkout \
  --runtime /absolute/owned/runtime-root \
  --validate-launch /absolute/reviewed/launch.json \
  --output /absolute/owned/launch-static.receipt.json
```

This verifies the existing original sealed runtime and packaged driver, checks
launch syntax and source identity, and records the later `configuration_sha256`
in a **new external** receipt. It allocates no second runtime, starts no process
from that runtime, and never changes the original package receipt. G remains
responsible for actual input-tree bytes, its approval and physical namespace.

A launch specification for an inert inventory command can use:

```json
{"format":"recall-gateway-launch/v1","command":["/usr/bin/python3","-I","/opt/recall/scripts/qualification/primary_provider_driver.py"],"environment":{"PIPELINE_SKILLS_DIR":"/opt/recall/share/native/skills"},"bindings":[]}
```

An actual primary run needs the reviewed in-namespace service orchestrator, both
persistent backend stores, explicit `migrate` before `serve`, configured spaces,
the explicit named bootstrap token profile and ordinary current service grants.
It must bind actual startup observations into B's installed runtime receipt and
G's approved context. Neither this packager nor an inventory command fabricates
`qualified:true`, starts that run, or produces final ledger settlement. The
primary driver is a public workload collector, not a service launcher.

Execution must still use G's actual approved launcher with the exact runtime,
launch spec, manifest, approval and dedicated credential FD. The one authorized
minimal host probe on 2026-10-07 failed immediately with
`bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted`; the orchestration
receipt records the exact command and 0.002-second refusal. The lead identified
host AppArmor/user-namespace restrictions. Until externally resolved, isolated
execution is blocked. No sysctl, capability change or host-network fallback is
part of this package.

Offline controls are self-contained and do not instantiate sockets or execute
inspection tools/candidates:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/qualification -p runtime_package_test.py -v
```

They cover supplied-only alias materialization, path escapes, missing targets,
cycles, collisions, file modes, exact hashes, truncation, deadline/size/allocation
bounds, all-directory entry accounting, strict JSON and launch environment,
plus mocked ELF closure/RPATH/glibc refusal, missing or undefined built-in module
initializers and missing, misplaced or non-ELF OpenSSL extensions. They do not qualify a real package,
Debian interpreter, dynamic loader, binary, service or gateway namespace.

An explicitly retained paired artifact may name its original `source_commit` in
`paired_bundle`. The packager retains its original provenance and verifies that
the complete patched core, SDK and SurrealKV source inventories and pinned
upstream preparation recipe equal the current Recall checkout. The ordinary
same-commit default and all existing artifact hash, ELF and builder checks remain.
The sealed runtime records Recall and paired origins separately. This permits
reuse of the original corresponding server after unrelated Recall changes; it
does not claim the paired executable was rebuilt or has passed runtime tests.


Current-capacity packaging successor (Root-selected 2026-10-09):

The isolated host invocation admits the packager’s exact sibling directory before
importing runtime_package_io and runtime_package_capacity, then removes that path.
Use --capacity-binding PATH --capacity-binding-sha256 SHA256 with the retained
RECALL_ARTIFACT_BUDGET_FILE pointing to the original shared ledger. The binding
pins the existing actual-capacity adapter, mounted-disk receipt and B’s exact
sealed copy of the latest CLOSED build observation. Original 9,733 checks and
1,317,796,118,528 consumed bytes are the next cumulative base. The original shared
ledger is not rewritten, and historical nominal caps are not reinstated.

The same adapter is the sole observation writer during packaging. Limits.check
requires capacity_ok true after every ordinary observer checkpoint; a low floor
refuses packaging under the existing bounded child-cleanup path. Both original
root/target 120 GiB floors apply. Current packaging monitors only filesystems it
uses; it does not inspect Docker’s unrelated storage. Existing 300-second command,
2 GiB output/4 GiB pool, package counts and fixed git/dpkg-deb/readelf ownership
remain. Static launch validation requires the same binding and original packaged
observation, checks actual device/capacity/floor identity, and writes a read-only
comparison in its own receipt without changing historical or observer counters.

Eight new finite host-only controls cover isolated CLI imports, unchanged observer
state, low-capacity refusal, old-cap-independent static history, either low
filesystem, device/total/path/floor changes an unselected adapter and exact selected latest-seed lineage before sampling. These are
source-authored controls, not executed or semantic acceptance. Root alone runs
them. The previous 15 paired/MCP and 2 input-count results remain unchanged.


Failure evidence and input custody:

The first actual 3756 runtime package original (0530f2) exited78 with a generic
message and no stage. Its capacity and failure remain historical evidence.
Independent read of the retained input manifest found all77 source harness files
group-writable (76 mode0664, one0775), despite unchanged SHA/bytes and clean Git
content. The existing checked_file guard correctly rejects those modes. The
checkout owner must clear group/other write bits on exactly these reviewed files,
preserving bytes and executable flags; the check is not relaxed. The generic
original failure cannot prove which predicate it reached first.

The host packager now tracks source-owned phase labels and bounded numeric item
indices. A caught failure writes OUTPUT.failure.json exclusively, fsyncing both
file and parent. It retains exception class and at most16 source-relative
file/function/line frames (at most16KiB). Exception messages, source-line text,
locals, request/configuration payloads, argv and credentials are never recorded.
If the evidence file cannot be written, stderr reports only the source-owned
stage and diagnostic_not_written. Existing failure evidence is never overwritten.
Successful package output, admission, child ownership, deadlines, integrity and
semantic expectations are unchanged. A successor needs a fresh output/capacity
observation and the latest retained cumulative accounting; never reuse the failed
original paths or relabel that original as passed. Two new finite controls cover
private exception-text exclusion and preservation of existing failure evidence.

The next prospective package after failed0530f2 uses B’s carry2: exact immutable
original capacity observation0fbb69a4, checks9743 and consumed1320864620544, not
the preceding build-only counters. The selected carry explicitly distinguishes
failed packager ownership from the previous successful build baseline. Only the
carry digest/length pins change in runtime_package_capacity; its validation and
120GiB floors remain identical to the eight-control source. Original paths and
counters remain untouched, and the successor output path is fresh.

The runtime staging directory is an exclusively created sibling of its final destination. Debian inputs and the package pool remain under the temporary scratch directory. Runtime sealing remains0555, and publication renames within the same parent without reopening the sealed directory for writes. The existing unpublished-only cleanup removes the owned staging runtime after failure; a published tree survives any uncertain receipt write. Root's original c701 publication PermissionError remains a failed run. The successor uses a fresh destination and actual cumulative capacity carry; neither input manifest nor accepted artifact bytes need to change.

The primary launcher binds its original gateway manifest and launch specification
at `/gateway-manifest.json` and `/gateway-launch.json`. The packager reserves
both empty file targets before sealing the runtime root, alongside the bridge,
context, anchor and socket targets. This allows those read-only mounts without
creating files in an already read-only root. The mount-target control compares
the actual primary launcher arguments with the real packager's target
declarations through a recording output; it starts no namespace or runtime and
does not establish provider acceptance.
