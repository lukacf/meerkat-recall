# Original launch observations

`launch_context.py` is the shared source authority for
`recall-gateway-launch-anchor/v1` and `recall-gateway-launch-context/v1`.
It creates no execution approval, runtime qualification or provider settlement.
Its exact field sets are the module's `ANCHOR_FIELDS`, `CONTEXT_FIELDS`,
`PROCESS_FIELDS` and `CLOCK_FIELDS` constants.

After validating the original manifest, packaged runtime and launch specification,
the parent launcher reads the ledger's original clock and finite allocation. It
seals a new private anchor outside the worker's writable mount and binds it
read-only at `/launch-anchor.json`. Source commit and paired binary identity come
from the hash-bound runtime package and actual binary bytes. The configuration
hash remains the exact original launch specification hash. The original manifest
`run_id` is the context's `run_nonce`; startup callers must use that reviewed nonce.
All sequential backend phases share the original parent wall/monotonic deadlines
and provider count/cost allowance. This does not introduce a fresh backend budget.

The readonly bridge imports `/launch_context.py` by absolute path under isolated
Python. Before starting the bootstrapper, it verifies its own and the helper's
source hashes, the actual boot and separate network namespace, loopback-only
interfaces and IPv4/IPv6 routes. It persists exact raw proc observations and its
PID, parent, process group, session and start ticks. It publishes the original
context path/hash through `RECALL_GATEWAY_CONTEXT_PATH` and
`RECALL_GATEWAY_CONTEXT_SHA256`. No JWT key, provider key, accounting capability or
account identifier appears in these observations. Existing private source
captures and provider accounting stay separate.

C calls `read_context(path, checksum, expected=reviewed_identities,
immediate_parent=True)` before startup. B uses the same validator with
`immediate_parent=False` and additionally validates C's actual owned service and
bootstrap receipts. Expected identities must come from the original reviewed
inputs; passing the context's own identities back as expectations provides no
independent identity check. At minimum compare the original manifest, nonce,
source commit, native/paired binaries, runtime, gateway source and launch
configuration. The shared validator verifies the still-live bridge identity,
readonly original anchor, boot/namespace, actual current route restrictions,
original raw capture hashes, and unchanged absolute deadlines. A phase applies
its own smaller bound within those original deadlines. It never resets them.

The parent owns provider ledger enforcement. The context proves only observed
launch custody under the reviewed launcher/bridge and trusted host/kernel; it is
not protection against a compromised kernel, host or replacement trusted harness.
Missing runtime package provenance, incomplete raw observations, replaced process
identities, clock expiry, or unsupported namespace facilities refuse the launch.
There is no alternate host-network path. Startup/consumer observations are dynamic
and hashed after production; their future hashes cannot be approved in advance.

Offline controls use mock proc bytes/process identity and temporary private files,
with socket/process constructors forbidden. They do not establish actual namespace
availability, startup, provider execution, resource use or model quality. The
existing separate provider allocations and request semantics remain unchanged.


The optional launch specification member `inputs` has exactly `directory` (a
canonical absolute host path) and `inventory_sha256`. The parent reads private,
regular, symlink-free originals, matches the approved sorted inventory, and copies
it to a separate private run-owned tree before reading a provider credential.
There are at most 256 files, 8 MiB per file, 32 MiB in total, 1,024 tree entries
and sixteen path components. The sealed copy has 0400 files and 0500 directories;
it is bound read-only at `/inputs`, separately from the writable `/work` and
hash-bound runtime. Original and copied inventories must match before launch;
consumers verify the actual `/inputs` bytes against the anchor's
`input_inventory_sha256` (null when no static tree is configured). No new manifest
field or provider allocation is introduced: the original manifest already binds
the exact launch specification and therefore its input inventory hash.

The hash dependency is directed: runtime/code, then static plans/corpus, independent
review, immutable input inventory, launch specification, outer approved execution
manifest/anchor, and finally observed receipts. Inner static plans/reviews must not
contain their own review hash, the later launch specification hash, or the final
manifest/context hash. Configuration identity belongs in the original outer
anchor and actual startup receipt. In-runtime package provenance must likewise
exclude any future launch specification/input-manifest hash; those belong in
outer receipts. Static plans can name the already-complete runtime hash without
creating a cycle. The runtime packager owns this separation.

The launcher also binds exactly its saved `manifest.json` at
`/gateway-manifest.json` and saved `launch.json` at `/gateway-launch.json`, both
read-only. Their host paths are fixed relative to the original anchor directory;
there is no caller-selected extra mount or writable run-directory exposure. The
manifest hash covers its exact original bytes; `configuration_sha256` covers
the exact original launch specification bytes. `launch.json` is the outer
configuration, not a third independently reconstructed object. Both originals
are rechecked before credential access on creation and resume, remain bounded
to 128 KiB each, and are never re-serialized for downstream capture. Consumers
capture those raw files and reuse the same original input-inventory reference
when constructing the independently specified `recall-gateway-launch-inputs/v2`
wrapper. This supplies evidence for independent validation; mounting the files
does not itself verify downstream startup or grant execution approval.
