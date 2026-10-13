# Container boundary extension

`container_boundary_offline.py` extends the actual eight-case container lifecycle
in `container_offline.py`. It owns no separate product state, image builder,
provider override or database access. The independent expected outcomes live in
D's `tests/recall_preview/container-boundary-contract.json`; this extension
records each CB01–CB10 and CC01–CC06 outcome separately.

The `plan` command is inert. `run --allocation /absolute/allocation.json` requires
all base allocation fields, the base independent review, and a second
`boundary_review` path and `boundary_review_sha256`. The second review has format
`recall-container-boundary-review/v1`, reviewer `qualification`, approved true,
and exact `driver_sha256`, `expectations_sha256`, and `dependency_sha256` from
`plan`. Additional dependencies include the base driver, S's asynchronous
`recall_workload.py`, and D's contract. Base review already pins every transitive
utility. Missing or changed source fails before Docker initialization. Allocation
is supplied by central scheduling; source readiness is not runtime allocation.

The active overrides observe real acceptance with workers disabled, real
publication after restart, absent/stale coverage, actual read-only mutations,
stale generation withdrawal, separate real retraction, and original erasure.
HTTP errors and transported MCP tool errors retain distinct framing. Authentication
and current-space middleware refusals remain their original HTTP401/403.
The actual snapshot route is `/v1/system/snapshot/import`.

Restore runs on its own connection while three clients independently poll
readiness, A and B. Source clients alternate REST and MCP. Each request records
monotonic connect/write/body-completion intervals, absolute deadline, original
body bytes and hash, complete-framing status, and private capture identity.
S's bounded parser has an outer asyncio deadline over connection, headers and
body; no concurrent SIGALRM and no partial body is accepted as a result. The
normal deadline is 5 seconds, restore 120 seconds, the combined HTTP budget is
1000 per backend, and all private captures are bounded at 128 MiB (32 MiB reserved
for the base command collector, 96 MiB for reports and HTTP/inspection evidence).
The body bound270336 permits MCP wrapper overhead around the262144-byte DTO.
Backends run separately under their original1500-second allocations.

SIGTERM records cancellation and the existing work/command polling checks stop
new work. The signal handler does not raise asynchronous exceptions. Before
either entrypoint enters its original exact-label Docker cleanup, it transitions
to cleanup state; a first or repeated signal in that state cannot unwind cleanup
or renew its separate 90-second deadline. In-flight operations retain their
original bounded request deadlines. The outer owner still requires positive
Docker and process cleanup evidence; an incomplete cleanup remains unmet.

A replay sample returning A's payload fails. Independent B must match its full
baseline; during clean rollback it can match the complete pre-restore or backup
version, never mixed fields. A sampled barrier passes only when an actual
recovery-not-ready readiness interval overlaps a fully framed typed data
`not_ready` refusal, both inside the original restore interval. A fast unobserved
barrier is **unmet**. No finite sampling result claims continuous refusal. The
base flow independently verifies Ready, erased-A404, original journal identity,
and exact B after restore.

The Metadata fixture cannot qualify CC04's prerequisite positive lexical hit,
CB07's actual oversized Graph evidence or CB09's indexed provider-approval
revocation. Those are explicitly **unmet**, with their prerequisites preserved.
Native unavailable is attempted only after a healthy control by stopping the
owned paired server, then restarting that exact server; a timeout or different
error does not pass. Embedded unavailable remains unmet without an existing
supported failure control. Physical erasure remains separate and unmet pending
the independent export verdict plus PV/media/backup closure. Coverage and
public 404 never substitute for physical deletion.

`container_boundary_test.py` uses only injected transports, in-memory parser
observations and temporary private files. It prohibits subprocesses and real
network connections. Such tests qualify the driver logic only, not Recall,
Docker, a provider, replay timing, model quality or physical erasure.

The inspector collection seam requires the coherent E decoder and L `snapshot
inspect` CLI in the pinned image. The registered baseline, post-erasure before
restore, and post-replay exports are inspected by an owned network-disabled one-shot
container. Only its backup volume is mounted, read-only; no live data, auth or
journal is mounted. `--json --include-records --compact-records` output goes to a private
tmpfs file and is copied with the 32 MiB-plus-newline bound, avoiding Docker log
truncation. The driver checks the original manifest, complete row/table counts,
unique typed identities, and inclusion of all raw record/schema bodies. It
retains original report bytes for independent source/PV/media/backup predicates;
collecting this inventory does not itself pass physical erasure. No unsupported
parser result or partial report is accepted. This explicit invocation requires
`recall-snapshot-inspection-v2`; silently returning v1 is a failure. The v1
custody checker remains available for original captures.

The compact report retains all original rows, identities, schema and hashes.
Long String/Bytes leaves use typed SHA256 references into the original lossless
`blobs` dictionary. The collector retains these bytes without expanding the
inventory. D's independent decoder must validate canonical unpadded base64,
raw lengths/hashes, UTF8, all reference occurrences and original row hashes.
Both unique decoded dictionary bytes and logical expanded leaves across all
rows are bounded at 64 MiB; the 14,065,536-byte per-leaf bound remains. The
collector checks the declared dictionary shape and bounds only. This does not
qualify decoding or erasure; dictionary validity and physical predicates stay
with D. The 32 MiB report and combined 96 MiB evidence limits are unchanged.
Actual maximum admitted-source and Graph report sizes require central runtime
measurement before claiming coverage.
The separate five-second retention fixture keeps its original expiry/deletion
checks; it is not inspected because starting an inspector could outlast that
original deadline.

`physical-fixture.json` records actual A/B acceptances, their distinct named
spaces and full published Source/head/operation baselines. The normalized
Metadata request includes a stable synthetic URI canary unique to the original
run, space and revision. This provides positive Source and immutable input data
before erasure without claiming document or graph extraction. The shared A/B
external key remains intentional.

A separate offline one-shot mounts only the original owned journal read-only.
It copies `journal-id` and the `erasures` intent directory only, with a 128 KiB
archive bound, at most 16 regular 4 KiB JSON files, exact UUID filenames and four
permitted intent fields. Links, path escapes, duplicate files, extra fields and
other file names fail. It never copies a reference key or membership file.
Original intent bytes must remain unchanged. Exactly one new A intent must be
recorded between the observed erase start and the observed completed-status
response; the latter is a client observation, since the public DTO has no
server completion timestamp.

D owns `snapshot_report_oracle.py::evaluate_export_bytes(pre, post, replay, fixture)`
and all physical predicates. Its source and independent expectations document
are required hash-reviewed dependencies before execution. The driver supplies
all three original report byte strings, including their original whitespace and
trailing newline, and the actual fixture receipt. D independently checks capture
lengths/hashes and rejects duplicate JSON keys or nonfinite numbers before
decoding. The collector does not re-encode parsed reports. A failed export
component fails the physical case; a passed component still leaves the full gate
unmet until live PV/WAL/media/previous snapshots and registered backup closure
are independently covered. The driver does not implement a second predicate
authority. Complete capture limits apply to the three reports in aggregate.


The optional Graph phase uses the independently reviewed original plan and
literal recipes. It preserves the two original named-space configuration
baselines and creates the recipe's separate fixture space with an explicit
signed, grant-backed data principal. The only runtime template substitution is
the endpoint of the owned controlled provider. The actual configuration and
listener address are captured. Missing plans leave CC04, CB07 and CB09 unmet.

Graph descriptor `canonical_sha256` uses the original recipe manifest and shared
Graph/controlled-spy codec: sorted compact JSON, UTF-8, unescaped non-ASCII, no
terminal newline. The container report encoder has a different wire format and
does not define recipe identity. A closed control sends the unchanged original
five recipe descriptors through the container reader and shared validators,
then rejects a descriptor hashed with the report encoder.

The approved initial HTTP bridge proposal was withdrawn because the SDK
requires HTTPS for non-loopback provider endpoints. The replacement fixture
helper shares the exact owned Recall container network namespace and binds its
provider listener only to `127.0.0.1`. It retains G's controlled provider and
uses a private Unix socket for snapshot/release/close control through owned
`docker exec`; no extra TCP control listener is published. The product image
and SDK policy are unchanged. The helper is a separate supplied local image
built centrally from an already prepared Python3.11+ toolchain layer, with
`Dockerfile.graph-helper`. Actual copied helper/shared component hashes and
Python version are checked in an offline owned probe before the live helper.

The only host bind is a new private directory inside the original allocated run
directory, owned by the same unprivileged UID. It holds synthetic fixture
settings, controlled-provider captures, the Unix socket and a file-locked shared
budget. It contains no product volume or deployment credentials. Both processes
reserve calls and held/written bytes in this original ledger; corrupt state,
identity changes and exhausted budgets refuse without resetting counters.
Provider reservation release and written-byte charging use one locked update.
Every public/provider connection shares the same 1000-call/1500-second budget.
Public operation polls are spaced by 500ms inside that deadline. Public response
capture stays 270336 bytes. The controlled provider's separate 2MiB body cap covers
its finite 128-by-3072 float batch; every byte still shares the original 96MiB
evidence ceiling within the 128MiB total. The helper closes its listener and
owned tasks, and ordinary outer cleanup re-verifies and removes its container.

CC04 requires a positive independently expected original claim before the
complete empty-token query. The revised CB07 uses two separately inspected
Graph sources: the real limit20 request must return the small typed413, and a
new limit10 request must fit the independent byte bound while retaining full
original claim quotes, spans and immutable revision IDs. Each returned evidence
also verifies the required `rendered_span`, `rendered_quote_sha256` and
`rendered_text_sha256` against the complete original immutable GET and reviewed
literal input. The final required `doc_id` must equal both that immutable
revision's original `doc` and the actual published document's `id`; absent or
foreign parent identity refuses even when every quote and revision hash matches.
These three fixtures preserve raw bytes and add only a terminal
LF; this exact transform is required. Raw evidence remains the minimal original
cover even for a chunk including the synthetic LF, and wholly synthetic
evidence refuses. No generic renderer is duplicated in this adapter. The old unsupported
single262145-byte-quote case remains explicitly unmet. CB09 revokes the actual
current query binding after indexed publication, then requires both real
semantic-search refusals and zero subsequent provider sends. The shared spy is
closed on success, refusal, cancellation or a failed case.

Before allocation, D's review must pin `graph_plan_sha256`, the full
`graph_input_sha256` map and the explicit revised-case marker
`cb07-two-source-complete-evidence-v1`, alongside all original source dependency
hashes. The plan format is `recall-container-graph-plan/v1`, with `space`,
`configuration`, `spy`, `positive`, two `response_budget` recipe descriptors,
`arithmetic`, `positive_query`, `empty_query`, and `query_binding_id`. Recipe
descriptors retain both original file and canonical hashes; arithmetic retains
its original file hash. This is source implementation only. Mock controls do
not establish actual helper reachability, Graph publication or product gates.

The optional allocation additionally requires `graph_helper_image_id` as an
exact supplied local sha256 identity, repeated in D's original review. No build,
pull, extra image creation or actual fixture execution is performed by this
source implementation. The helper Dockerfile is a centrally prepared artifact
input; no compiler, image or provider run was performed in this worktree.

F's mock-only suite covers the shared ledger, atomic reservation conversion,
corrupt/foreign/expired budget refusal, original client authority, provider image
identity refusal, loopback/control/cleanup interfaces, and negative dual evidence
checks. It replaces G's interfaces with explicit doubles when exercising F's
helper, and denies process creation and network listener/client creation. G's
separately owned test suites are not imported or executed by these checks.
# Shared original snapshot custody parser

`scripts/qualification/snapshot_inspection_capture.py` contains the original
`checked_inspection(raw, original_manifest, *, expected_format=...)` custody
checks. The active container boundary driver delegates to that same function
and includes the new helper in its reviewed dependency inventory. Installed
full-workload controllers can receive the pure helper from their verified
source loader without importing the container driver or its runtime helpers.
The helper imports only standard-library JSON and regular-expression modules.

V1 and explicitly requested V2 retain their existing manifest, complete row,
identity, table count, schema and bounded dictionary checks. The report limit
remains 32MiB plus the CLI newline. Compact bytes remain unexpanded here; D's
independent oracle still owns decoding and physical-erasure acceptance. This
extraction introduces no process execution, new allowance or passing verdict.


## Original stopped-volume physical evidence

The boundary driver now retains complete raw archives of the stopped original
`data`, paired `database`, and registered `backups` volumes, plus the separately
mounted non-secret backup registry. It records four observations: before erase,
after acknowledged erase before replay, after replay, and after the original
observed backup deadlines. Both product processes must be stopped before a
read-only, offline helper captures them. The original 1,500-second clock,
400-command budget, and combined 128-MiB evidence bound still apply; the new
archives consume the existing 96-MiB boundary share. No separate clock or byte
allowance is created.

`physical_inventory.py` rejects incomplete tar framing, duplicate or escaping
names, links, special files, sparse payloads, missing parents, truncated members
and hidden trailing archives. Complete embedded checkpoint checksums and every
historical WAL frame are decoded with their original identity, sequence, checksum
and commit markers. The original independent export oracle supplies the physical
lineage, retained-control rules, strict terminal states and exact independent B
baseline. Every decoded row is checked; a later deletion does not excuse earlier
payload bytes that still remain in a WAL frame.

Only `reference-membership-index.json`, `reference-memberships`, optional
`history-commitments`, the original journal UUID and the existing fingerprint
leave the separate journal mount.
The private `reference-key.json` is never opened, copied, parsed or hashed.
Membership files bind their exact original intents and checksums, and their
complete token sets must match the actual live rows and guard counts.
The optional history directory has an independently captured presence flag.
Its complete file inventory must exactly match `history_intents` in the checked
original index. Each checked proof binds that same original intent, journal and
fingerprint, and contains only bounded scope hashes, observed-history hashes and
erased-version commitments. Missing acknowledged files, unindexed or temporary
files, unknown fields, substituted checksum domains and shrinking acknowledged
sets cannot qualify a completed phase. The collector never reads a secret key.

Before expiry, a retained source-bearing backup is exempted only by its original
observed generation, exact payload/manifest hashes and unchanged deadline.
Every observed generation is checked after its own original expiry. The driver
waits only if those deadlines fit its remaining original allocation, restarts the
ordinary service to invoke its existing cleanup, then inventories actual files.
It never edits clocks, registry entries, manifests or product files. A genuine
seven-day raw generation beyond the run's window remains incomplete evidence.
The existing five-second fixture remains separate and unchanged.

An export-component pass alone still cannot complete the physical gate. Unknown
file formats, compressed native engine generations without independent decode
or verified reclamation, incomplete captures or unexplained files remain
explicitly unmet. A confirmed retained source row, canary or expired original
payload is a failure even if other evidence is incomplete. This original Metadata
fixture does not exercise general managed-media deletion; complete media-file
inventory and the actual original lineage are required, without inventing a
new mandatory positive-media fixture. All helpers and controls remain source
only until the centrally scheduled actual run produces the original captures.

For the original paired `file:/data/database.db` RocksDB recipe, the collector
also records actual device, inode, byte size, link count and file type for every
archived data/database/backup file and directory. These NUL-framed observations
come from a separate read-only helper after both writers stop. A strict physical
checkpoint receipt must bind the actual fresh directory and native `IDENTITY`,
the original erase or restore interval, completed old-generation retirement,
and its previous receipt. The complete volume must contain only recognized
native files and the exact payload-free ownership controls. An opaque extra file
or an unexplained sibling generation remains unmet. Normal later flushes and
shutdown may change current native files; a deleted inode number is allowed to
be reused. The proof relies on the reviewed native generation replacement and
complete subsequent custody, alongside the independent complete A/B export
checks. A compressed-byte canary scan alone cannot qualify retirement.

After Recall stops and before its paired server stops, the driver runs the
fixed five-statement read-only query from `native_private_membership.py` against
that existing server on its original private network. A one-shot helper receives
only the synthetic paired-server credentials and no product or journal mounts.
Its complete bounded HTTP body is copied from private tmpfs and retains its
original bytes and query hash. The decoder checks native type witnesses, complete
row and object-key inventories, schema, stable native IDs and exact journal
membership sets. Exported guards must independently report those same counts.
This private projection remains excluded from ordinary backups. Known private
payload is a failure even if another physical observation is incomplete.

All added commands, private captures, history archives and metadata share the
existing original clock and evidence limits. No extra erase checkpoint, manual
file cleanup, backup deadline change or additional allocation is introduced.
The SurrealKV receipt shape has a separate strict decoder control; the original
RocksDB container recipe does not qualify SurrealKV version history. Pure composed
controls do not establish actual SurrealQL syntax, server responses or native
retirement. Those prerequisites remain with the central actual qualification run.

Native registered backups additionally contain the exact `history-origin.json`
sidecar. The independent collector checks its complete canonical payload-free
shape, opaque content and causal commitments, original journal fingerprint,
fixed paired scope, snapshot ID, payload hash, and a hash of every original
manifest field, including retention. The sidecar becomes an accounted member of
that same observed backup generation and must disappear by its original deadline.
Missing or unexplained sidecars cannot qualify the generation.

The original journal's optional `restore_origins_required` index field anchors
`imported-history-origins.json`. The stopped helper observes this exact file's
presence, copies it within the same original evidence allocation, and never
copies the secret reference key. Its complete canonical scope and version/time
bindings must preserve every previously acknowledged origin across later phases.
Losing the file, its anchor, a scope, or an exact prior binding is not accepted.
Opaque commitment sets remain bounded; unknown fields, duplicate values and
noncanonical bytes refuse classification. A known original canary remains a
failure even when its accompanying index is malformed.

This accounting explicitly reports that the collector has **not verified the
HMAC**. The product's original journal startup and pre-restore verification own
secret authentication and causal authority. Recognizing bounded opaque control
bytes never grants permission to rewrite historical versions or establishes
that a backup was authenticated. Embedded backups keep their original format.
