# Actual isolated runtime startup receipts

`runtime_package_bootstrap.py` is inert by default. An inventory invocation does
not inspect credentials, start a service, open a TCP connection or authorize a
provider. The source is intended to run only as the actual child of G's reviewed
bridge in a packaged, read-only `/opt/recall` runtime. No host-service fallback is
supported. Source presence and offline tests do not qualify installed behavior.

The receipt `recall-runtime-bootstrap/v1` reports `status: bootstrapped` and scope
`authenticated-clean-persistent-startup-only`; `qualified` and
`product_acceptance` are always false. It records observations needed before B's
original primary workload. D owns independent acceptance and the original corpus;
G owns physical isolation, provider approval, accounting and the whole-run clock.

## Acyclic input order

1. Seal the real runtime, executable/source identities and reviewed helper files.
2. Write immutable bootstrap plans and the original corpus, expectations, provider
   proposal, named-space creation requests and conditional-case decisions. Plans
   contain the runtime hash but **no** review reference, primary-template reference,
   launch-spec hash, future observation hash or input-tree hash.
3. Write one primary-manifest template per backend referencing those static inputs.
   It has the original explicit execution/dispatch permissions but no review
   reference, observations, token paths, output directory or configuration hash.
4. D independently reviews the actual plan/template/input bytes and source hashes.
   Its original `recall-primary-driver-review/v1` includes `primary_template_sha256`,
   all immutable `inputs` hashes including the **raw** bootstrap-plan hash, the
   complete common dependency map, original backend/nonce/endpoint and exact
   observation contracts. No canonical projection exception or self-hash is needed.
5. G binds that complete private input tree with `inputs: {directory,
   inventory_sha256}` in the outer launch specification and approved manifest.
   G stages and mounts the original tree read-only at `/inputs` before the bridge.
6. The bridge produces actual launch/namespace/process observations. C validates
   them with G's actual `read_context(..., immediate_parent=True)` and obtains
   `configuration_sha256` from the original read-only outer anchor. It never
   writes an approval or fabricates a future receipt hash.

D's `expected_launch` contains the immutable G hash fields except the later
`manifest_sha256` and `configuration_sha256`, plus source commit, run nonce and
`allocation_sha256: null` for the original primary allocation. G's validator
checks the actual outer manifest/launch/input identities, original read-only
anchor, raw IPv4/IPv6/interface captures, still-live original bridge identity,
namespace and boot ID. C and B consume that one authority.

The static bootstrap-plan format is `recall-runtime-bootstrap-plan/v1`; its exact
fields are defined by `validate_plan` in `runtime_package_bootstrap_contract.py`:
source commit; actual Recall and paired binary hashes; runtime hash; backend;
endpoint `http://127.0.0.1:43130`; original run nonce; expiry; exact two creation
requests; explicit service scopes; complete dependency hashes; and new private
`/work/bootstrap-NONCE-BACKEND` directory. Frozen classification remains
`household-policy` level 3 and `business-policy` level 7. Real processing bindings,
model identity and extraction guidance come from the independently reviewed
creation requests. Plans contain no credential values.

The primary template also requires an additive `inputs.rendered_map` private
read-only reference. D's independent review binds its exact raw bytes alongside
the original corpus; no original corpus, query, proposal or allocation hash changes.
The map separately specifies rendered Doc/DocRevision bytes and default tree
spans. B checks those expectations against actual output; C checks the original
reviewed input hashes before any startup. Missing or changed map bytes refuse the
phase. Never trim observations or replace raw source hashes with rendered hashes.

The exact CLI uses explicit triples, once per original backend:

```text
/usr/bin/python3 -I /opt/recall/scripts/qualification/runtime_package_bootstrap.py
  --execute-authorized
  --phase /inputs/elephant-plan.json /inputs/elephant-review.json /inputs/elephant-primary-template.json
  --phase /inputs/surreal-plan.json /inputs/surreal-review.json /inputs/surreal-primary-template.json
```

This is a launch-spec example, not execution approval. At most two distinct
backend phases run sequentially in the same bridge child and original gateway
allocation. Only one service cohort uses the private ports at a time. Neither
phase creates a fresh whole-run allowance. Full primary qualification still
requires both original backends; a one-phase startup receipt cannot substitute.

## Actual startup and original public controls

C creates a fresh private persistent directory and uses only the bridge's fresh
synthetic API signing material, copied to a private key file. The actual packaged
CLI issues the global runtime operator, exact named management bootstrap tokens,
and ordinary/read-only named service tokens. Original effective scopes and
clearances are explicit. No global data clearance or ambient provider key is used.
All provider connection references must resolve to G's newly minted loopback
capabilities; the only endpoint is the actual `127.0.0.1:43129/v1` relay.

For `surreal-db`, the packaged `recall-surreal` runs at `127.0.0.1:43131` with a
fresh private `file:` store and fresh synthetic database credentials. The actual
Recall `migrate` command initializes the schema before `serve`. For `elephant-db`,
`serve --server-only` must initially report unconfigured; the explicit authenticated
MCP `initialize_runtime` initializes the fresh persistent storage. Embedded
`migrate` is not supported and is never substituted. Serve does not apply DDL.

The driver calls actual `POST /v1/spaces`, then captures the genuine generated
SpaceConfig IDs/timestamps and independent readbacks. It captures ordinary
missing-grant denials, creates and reads the live ordinary/read-only grants,
checks both exact classified configurations and empty inventories, verifies
bootstrap-data and read-only-write denial, revokes/restores the actual service
grant with generation CAS, checks both cross-space directions and unauthenticated
readiness. It then explicitly enables workers after bind and requires actual
worker/semantic-ingest/recovery readiness plus final empty inventories. Startup
submits no source, search, model probe, wiki or reprocess job.

Every request has one persisted original intent before its one send. Readiness
polls are observations; mutations are not retried. The actual S bounded HTTP
reader captures raw response bytes, body hash, framing status and timestamps.
Synthetic bearer values are absent from stored request intents; credential roles
identify the caller. Each receipt check references its exact private original
request/response captures. B independently validates those hashes and actual
responses; a count or copied boolean is insufficient.

`bootstrap_process` is G's actual process identity shape. Each service records
PID, parent PID, process group, session, start ticks, executable path and exact
binary hash. B checks the still-live original bridge -> C -> service/collector
chain. `process_commands` references a separately hash-bound private command
journal. Completed CLI/migration logs have exact hashes; live service logs are
explicitly unfinished. Schema evidence references the actual migration process
or original initialization response. Final cleanup has a separate record and
never rewrites the startup receipt.

## Bounds and consumer handoff

Startup has a 300-second ceiling inside a once-created backend allocation of at
most 2,700 seconds, itself bounded by G's original whole-run wall and monotonic
deadlines. The same backend clock includes input/context verification, CLI calls,
service initialization and handoff. B consumes the remaining original absolute
deadline. C reserves 75 seconds for cleanup; it does not grant that reserve to B
as additional work time. Each HTTP request is at most 15 seconds, 128 KiB request,
256 KiB response; startup permits 160 requests, 16 MiB captures, 16 processes and
4 MiB per process log stream. Parent G count/cost limits remain authoritative.

C writes a final primary manifest by copying the reviewed template and adding
only the genuine D review reference, actual context/startup receipt references
and hashes, original observed launch configuration hash, ordinary token paths
and a new private capture directory. It never manufactures permission flags or
D's review. The real packaged B collector runs as C's owned child. It receives
no API signing key or provider credential. The actual service retains only its
approved synthetic relay capabilities.

All children use S's retained-leader `OwnedProcess` helper, bounded private logs,
new sessions and owned process-group cleanup. C requests SIGTERM and bounded
clean drain, then kills/reaps only its own process groups if necessary. Cleanup
records forced termination, nonzero service exit, signal/reap errors and unfinished
logs as failures even when a subsequent kill successfully reaps the process.
Every owned handle is considered despite an earlier cleanup error. Wait and log
drain bounds consume the original remaining deadline; expiry permits only signals
and nonblocking reap, never a fresh five-second allowance. This requires the shared
S `OwnedProcess.wait` to bound its entire wait, reap and log drain. A failed terminal
cleanup suppresses the next backend phase and records `status: incomplete` and
`cleanup_complete: false`, without rewriting the earlier startup receipt. A
constructor must also retain and clean its child if log-thread initialization fails.
C passes S's `initialization_deadline` as the lesser of five seconds and the
original remaining deadline. If the constructor raises with `owned_process`, C
retains that exact handle and typed initialization failure in its command/cleanup
journal, including an already reaped child. It never invents a handle for a
pre-spawn failure or stores exception messages that could contain secret values.
If the final group signal fails, C preserves the unreaped leader and records
incomplete cleanup while visiting the other handles. It does not release that
reserved PID while descendants may remain alive. G's enclosing namespace custody
remains the final containment boundary; that boundary cannot turn the failed
cleanup into successful qualification.
Cleanup failure or an incomplete command/log/capture is not qualification. The private
original captures remain for independent review. Current host namespace failure
and unfinished composed artifact/consumer validation continue to block execution.

Offline tests use synthetic in-memory data, temporary files and mocked transport/
processes. They do not run a candidate, contact a model, open a TCP service or
certify real persistence, startup or provider behavior. Complete actual producer
source, B's matching dynamic validator and G's input/context producer must be
composed and independently reviewed before any scheduled execution.

Bootstrap accepts the two original classified policy families. Primary uses
`semantics-household` with `synthetic-household` at level 3 and
`semantics-business` with `synthetic-business` at level 7, exactly as frozen in
the primary corpus. These reserved names cannot carry workload policies, and
primary policies cannot be assigned to other names. The retained workload pair
uses `household-policy` at level 3 and `business-policy` at level 7 with its
existing bounded named spaces. Each plan must contain one complete family;
mixed families, duplicate names and swapped primary envelopes refuse. Labels,
handling, generation, guidance and the separate allocation checks retain their
original contracts. Accepting either source envelope does not authorize startup
or supply the independent conditional TM/wiki case review.

A reviewed primary plan for scoped article preparation carries `scoped_wiki:
true` and explicitly requests both `manage:wiki` and `write:entity`. The latter
is needed by the existing governed Definition and article compiler APIs; it is
absent from core/default scopes and is rejected without that marker or for the
workload policy family. False or nonboolean markers, missing companion scopes
and broader write/management scopes refuse. The primary collector separately
binds the marked plan to its reviewed scoped article cases and actual observed
Definition IDs. This path exercises writing and verification; it does not
establish execution of the full-graph planner or organizer.
Before constructing startup or granting credentials, `phase_inputs` also checks
that the exact hash-reviewed conditional input selects scoped compilation if
and only if the plan has the marker. It uses the already verified bytes without
a second input read. The collector still validates the complete case recipe and
rechecks that binding before conditional work.

The reusable startup primitives distinguish the primary backend ceiling (2,700
seconds, still the default) from the original D28 whole allocation (7,800
seconds, explicit only). Both remain bounded by the original whole deadline and
wall expiry; the bootstrap subphase stays 300 seconds with the same cleanup
reserve. Selecting the D28 ceiling does not open a preparation/measurement
cohort, extend their separate limits, reset a clock or authorize execution.
The D28 context caller must select the exact original allocation SHA; primary
continues to require an absent allocation. Ordinary named tokens can cover the
remaining original allocation, within the existing 31-day CLI ceiling. Bootstrap
and global management tokens remain at most one hour. Expired or invalid clock
inputs refuse issuance instead of manufacturing one extra second.

The actual v1 receipt uses `qualification_scope`, and its historical
`artifact_sha256`/`paired_artifact_sha256` fields identify the packaged executable
bytes checked against the owned processes. A new dynamic envelope with separate
archive and binary identities must bind these meanings explicitly to original
raw evidence; it cannot relabel the receipt or infer archive identity from an
executable. These reusable primitives alone do not provide the D28 dynamic
component, offline prerequisite or v2 consumer handoff.
