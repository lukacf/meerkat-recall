# Original runtime custody producer

Primary `recall-primary-provider-run/v2` and its independently approved
`recall-primary-driver-review/v2` require immutable `runtime_review` and
`runtime_original_inputs` inputs. The latter is exactly:

```json
{
  "format": "recall-runtime-original-inputs/v2",
  "review": {"sha256": "<original hash>", "bytes": 123, "kind": "runtime-review"},
  "installed_offline": {"sha256": "<original hash>", "bytes": 456, "kind": "installed-offline-evidence"},
  "files": [{"path": "/inputs/<original file>", "ref": {"sha256": "<original hash>", "bytes": 123, "kind": "<original kind>"}}]
}
```

The displayed hashes/sizes are schema placeholders. Every actual original review,
installed carrier and transitive required capture must appear with its actual
typed hash/size/kind. They must already exist under the immutable reviewed input
mount. C does not reconstruct an installed carrier or claim that its nineteen
offline cases prove current provider/paired-runtime behavior.

After actual startup, `execute_phase` captures original process/network/configuration
custody and writes `recall-runtime-custody/v2`, with exactly `format`,
`observed_runtime`, `controller_context`, `files`. The observation is the complete
D `recall-dynamic-runtime-evidence/v2` envelope. The on-disk controller wrapper
contains exactly `ref`, `observed_wall`, `observed_monotonic_ns`; the consumer
rehydrates the original context and raw bytes from the referenced file. There is
no base64 or replacement serialization of an original receipt.

Every file is bounded and hash-checked again immediately before the sidecar is
published. The full location inventory is checked before bodies are read:
16,384 files, 64 MiB per file and 352 MiB of distinct runtime/installed originals;
the sidecar itself is at most 8 MiB. Producer verification holds one input body
at a time; the primary consumer uses its bounded original-evidence mapping.
These are reading limits for existing retained captures, not larger capture,
process, time or provider allocations. G's separate 20 GiB provider accounting
channel is outside this startup inventory.

The existing v1 bootstrap and G context bytes remain unchanged and unqualified.
Historical v1 templates remain parseable but cannot execute bootstrap-only
qualification. A v2 manifest cannot be produced without actual custody, and
joining observations never grants execution or provider permission. After
writing custody, the original G context and deadline are checked again before
the existing owned consumer process starts. Its sole L runtime predicate must
pass before credentials/workload requests; cleanup remains C's original owned
obligation even when the consumer refuses evidence.

Archive and executable identities stay distinct: the v2 archive identity comes
from the original D review, while v1 `artifact_sha256` is matched to v2
`binary_sha256`. No installed nonce/runtime/paired proof is invented. Missing
prerequisites or changed current identity refuse. The in-memory controls block
process/socket execution and exercise original-byte retention, reference loss,
late mutation, conflicting types, changed invocation and version/permission
boundaries. They do not qualify an installed runtime.

The legacy name `paired_artifact_sha256` identifies the actual packaged
`recall-surreal` **executable** in both the original G/bootstrap records and the
dynamic identity. The producer compares it to the independently reviewed paired
executable digest before writing custody. It does not rename that field or infer
the paired release archive's identity. Paired archive provenance and actual
paired-server behavior remain separate qualification obligations.

The active bootstrap now supplies the installed owner's shared `ProcessProof`
to the same `OwnedProcess` constructor before its actual launch. The proof sees
the original argv, environment **names**, executable input, unreaped launch
stat/executable/stat observations and exact allocation deadline. Actual wait,
signals, reaping and EOF events come from that shared custody primitive. A fast
CLI that is already a zombie retains that truthful observation; C never rebuilds
its launch identity after reaping. The shared helper's two MiB metadata sublimit
and 512-event bound are charged to the original bootstrap sixteen MiB capture
allocation through the same locked counter as HTTP captures.

The original `recall-runtime-process-commands/v1` file is unchanged. The dynamic
envelope's `command_inventory` points to an additive
`recall-dynamic-startup-command-inventory/v2` object containing `commands` (that
original file's typed reference) and ordered `process_proofs` references. Each
proof is the actual shared snapshot; every event/stat/terminal-log reference is
included as original bytes. Terminal logs use the shared `process-log` kind.
Missing original launch proof refuses even when setup exited successfully.

Current custody returns a lazy `Mapping` of digest to original bytes. Its index
retains only paths and typed references; membership and iteration do not read
bodies. Every lookup privately rereads, bounds and hashes the requested original
files, including any duplicate paths for the same digest. There is no body cache
and no materialization of the full 148 MiB current-capture inventory. Individual
originals retain their existing limits; comparing duplicate bodies transiently
holds at most two individually bounded bodies. Metadata retains the two MiB
limit, while process logs retain the original four MiB per-stream limit in both
the completed-command and shared-journal passes. Aggregate limits are unchanged.

Before the shared process constructor, its five-second initialization deadline
is additionally clamped to the shared helper's conservative conversion of the
original integer allocation deadline. Floating-point rounding cannot extend
that original deadline, and no tolerance or new cleanup allowance is added.

C's dynamic entry loads for the workload helper and actual G context helper
read a bounded original source body, verify its reviewed digest, and execute
exactly those bytes. Module name, file and import specification remain attached.
They do not ask a source loader to reread the path or execute cached bytecode
after verification. The immutable runtime mount and complete transitive helper
inventory remain required; this change does not grant trust to additional code.

`LiveStartup.capture_original_custody()` also returns `current_custody`, a typed
`runtime-current-custody` reference. Its object is exactly
`{format,current_processes,capture_inventory,cleanup_obligations,worker_recipe_capture,provider_recipe_captures,outer_launch}`,
with format `recall-dynamic-current-runtime-custody/v2`. Each recipe capture is
`{request_index,request,response}` with original typed byte references; provider
captures are keyed by both named spaces. This includes actual later boundary
reads in the capture inventory while retaining the frozen startup receipt and
its exact original capture prefix. S must await `revalidate_worker_recipe()`
before recapturing the preparation boundary. It uses that new reference in the
shared runtime chain, without copying current metadata into an old receipt.

The initial dynamic runtime envelope and every current-custody object also carry
`outer_launch`, a `gateway-launch-inputs` typed reference. Its exact object is
`{format,manifest,spec,input_inventory}`, with format
`recall-gateway-launch-inputs/v2`. `manifest` references the original raw
`/gateway-manifest.json` (`gateway-manifest`), and `spec` references the original
raw `/gateway-launch.json` (`gateway-launch-spec`). G mounts only its originally
saved `manifest.json` and `launch.json` at those fixed read-only paths. The launch
specification is the outer configuration; no third configuration is inferred.

C privately reads each original under its existing 131,072-byte limit, compares
its exact SHA256 with the original anchor's `manifest_sha256` or
`configuration_sha256`, and retains those raw bytes through the lazy original
mapping. Whitespace is preserved. The wrapper is at most 16,384 bytes and reuses
exactly `current_processes.input_inventory`; it creates no second inventory
authority. Both original files and the wrapper belong to the same current
capture inventory and unchanged aggregate capture bounds. They are reread and
hashed before current custody is published, so a changed file cannot borrow an
earlier observation. The original v1 startup receipt and static 15-field launch
expectations remain unchanged.

The outer-file capture binds byte identity only. L independently parses the
original manifest/specification and checks their source, artifact, allocation,
bindings, command, environment and input relationships. Matching an anchor or a
receipt without those originals does not establish those relationships. The
in-memory controls cover original whitespace retention at initial and later
boundaries, exact inventory-reference reuse, changed/missing/oversized originals,
changes during process observation, and refusal of additional caller-selected
mounts or reinterpreted kinds. All process/network observations remain mocked.
