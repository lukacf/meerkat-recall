# Offline container and persistent-volume qualification

`python3 scripts/qualification/container_offline.py plan` is inert. Importing the
module does not contact Docker. This source implements D21's reviewed lifecycle
inventory for **each** backend; it does not build an image or prove those cases
merely by existing or passing its mocked tests.

Execution belongs to the central allocator under Luka's existing mandate. There
is no additional user approval request. Root supplies a new private directory,
an unexpired run allocation and D's independent source/case review. Never run
this driver during the shared resource-sensitive window without that allocation.
No real Docker, service or provider execution was performed while authoring it.

The allocation is an absolute canonical JSON file with format
`recall-container-run/v1`, `execution_authorized: true`, canonical UUIDv4 `run_id`,
`backend` (`elephant-db` or `surreal-db`), UTC `expires_at` within two hours,
new absolute `run_directory` outside projects/runtimes, canonical local
`docker_socket` (usually `/run/docker.sock`), exact `source_commit`, `source_tree`,
`source_tar_sha256`, and both local `recall_image_id`/`paired_image_id` in
`sha256:<64 lowercase hex>` form. It also supplies trusted lowercase SHA-256s:
`recall_binary_sha256`, `paired_binary_sha256`, `recall_provenance_sha256`,
`paired_source_provenance_sha256`, `paired_build_provenance_sha256`.

`review` points to D's absolute canonical review JSON; `review_sha256` binds its
bytes. Review format `recall-container-review/v1` requires
`reviewer: "qualification"`, `approved: true`, and exact `driver_sha256`,
`expectations_sha256` (from `plan`) plus `utility_sha256`. The allocation repeats
that same utility map, binding these files from the integrated source tree:

- `scripts/qualification/installed_linux_offline.py`
- `scripts/qualification/recall_preview.py`
- `scripts/release/artifact.py`
- `tests/recall_preview/fixtures.py`
- `tests/recall_preview/matrix.py`
- `tests/recall_preview/oracle.py`

Only S's frozen pure `mcp_value`, `typed_denial` and D's total-deadline helper are
called. No private driver/process class is reused. These dependencies must be
integrated before an actual run; mocked tests require none of them.

Use `python3 scripts/qualification/container_offline.py run --allocation PATH`
only for that allocation. Each backend receives a separate run UUID/directory and
resource set. The run uses no ambient Docker context, registry credentials,
provider keys or existing service. Paired storage has no host port. Recall exposes
only a Docker-assigned loopback port. Synthetic JWTs last 30 minutes; the full
case flow is bounded to 25 minutes plus 90 seconds for owned cleanup.

All containers have read-only root filesystems, PID/memory/CPU/log/tmp bounds and
no privilege escalation. Short offline root helpers initialize/copy **owned**
volumes with only CHOWN/DAC_OVERRIDE/FOWNER capabilities. No host directory or
Docker socket is mounted into any container. Every resource has exact run,
nonce and purpose labels. Cleanup checks those labels and immutable IDs again;
it records uncertainty instead of deleting an unproven resource. Free-space
samples enforce the shared 250 GiB floor at launch boundaries, not a hard volume
quota. Binary copies are finite tar streams inspected without host extraction.

Runtime setup uses actual retained APIs: fresh remote `migrate`; embedded
`initialize_runtime`; normalized `POST /v1/sources/ingest` with explicit Metadata
mode; exact original operation/head polling; actual worker enablement; actual
registered snapshot export/import; actual public erase. Pure Metadata does not
select graph extraction or embeddings and is not a semantic-quality test.
Missing-journal negatives operate on cold copies, never the only original store.
The original independent journal is never regenerated or taken from a backup.
No private signing/reference key bytes appear in evidence.

Evidence files are exclusive 0600 files inside a 0700 run directory. They include
bounded raw synthetic HTTP responses with framing, completeness and hashes;
monotonic starts/ends/deadlines; sanitized command vectors and output hashes;
image/binary/source/backend identities; original acknowledged operations;
actual backup registration/expiry; and complete independent-space before/after
observations. Readiness proves the product's schema verification ran, not an
invented HTTP migration-history hash. The replay case observes the first Ready
sample after restore and then source absence; it does not prove a continuously
observed outage or independently qualify physical source deletion/OB3.

`observations.json` enumerates **every** case as `pass`, `fail` or `not_executed`.
A failed/absent case or uncertain cleanup prevents this backend's acceptance.
D owns the independent verdict. The other backend, live provider quality, native
installation and crash/ENOSPC qualification remain separate gates.

Run the source-only mocked suite with:
`python3 -m unittest discover -s tests/recall_preview_semantics -p 'container_offline_test.py'`.
It launches no Docker, Cargo, service, model, or subprocess and cannot establish
product acceptance. No fixtures substitute a published product receipt in an
actual driver run.

Persisted space configuration is read through the actual retained
`GET /v1/spaces/{space}` route after creation and compared after pending restart,
original-journal clone fallback, clean restore, erasure replay and retention
restart. Baselines include distinct intended model/dimensions/purpose/skill,
complete security and processing authority, and every other returned field.
Only top-level `updated_at` and `processing.index_generation` are omitted from
identity comparison: content publication advances those storage-owned clocks.
Raw captures retain both. Security, policy, configuration generation, profiles
and bindings are never omitted. Registered backup `data.snapshot` bytes are
copied through the bounded private tar path and checked against actual manifest
length/SHA-256, backend and original journal before restoration.

Credential roles follow the actual service-token CLI contract. The global
operator receives only explicit management scopes and no space or data clearance;
only it initializes runtime and exports/imports snapshots. Named service and
single-space credentials receive explicit spaces, a default level-0 clearance,
source/status/config-read scopes and matching current grants. Ordinary data
requests never use the global token. Space creation and grant administration
still apply record ABAC. Consequently this fresh-bootstrap fixture uses the
supported `default` policy and level 0 for both configuration envelopes; it does
not qualify custom-policy or classified-space bootstrap. Named identities,
models, dimensions, purpose, skill and processing configuration remain distinct,
and full security/configuration persistence checks remain strict.
