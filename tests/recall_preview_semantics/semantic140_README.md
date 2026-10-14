# Closed semantic140 qualification profile

`semantic140-v1` adds a separately bounded semantic workload to the existing
qualification collectors. The original implicit `d28-v1` mode retains its
50 callers, 200 requests, original six input files and original allocation.
Neither profile grants execution. The successor data files explicitly retain
`execution_authorized: false`, `provider_dispatch_authorized: false` and
`live_ready: false` where applicable.

The six `tests/recall_preview/semantic140-*.json` files form one immutable
profile. The new corpus has a separately hashed 560-request schedule, while
every original source, query, known answer, space and metric object remains
unchanged. Each space has 70 callers; each caller inherits the original four
ordered requests for source `local_caller modulo 25`. All 100 known-answer
queries participate: 80 appear six times and 20 appear four times, frozen before
measurement. The independent oracle reports per-query results as well as the
original weighted per-request MRR; there is no additional per-query threshold.

The closed helper binds exact input hashes and the full canonical contract and
corpus. Arbitrary caller counts, caller-provided quotas, mixed old/new inputs,
unknown profile IDs and a successor allocation under the old profile refuse.
New phase manifests, preparation observations, measurement observations,
independent reviews and orchestration inputs explicitly name `semantic140-v1`.
Existing old shapes remain available. Prepared-component identities retain
their original exact input/allocation hash fields.

The public invocation remains `semantic_workload.py --manifest PATH
--manifest-sha256 HASH --execute-reviewed`. A successor orchestration manifest
uses the existing `recall-d28-orchestration/v2` format with the additional exact
`profile_id: semantic140-v1`, and `/work/semantic140-<original nonce>` output.
The independent review selects the same exact new allocation through
`controller_policy.expected_anchor.allocation_sha256`. All actual source,
artifact, config, credential, namespace, original clock and spending approvals
remain required inputs; this source does not manufacture them.

The gateway successor uses the separately reviewed
`recall-semantic140-embedding-manifest/v1` and accounting policy
`separate-exclusive-semantic140-embedding-reservation/v1`. Only this profile
uses `recall-gateway-launch-anchor/v2`, with the one additional `profile_id`
field; no `profile_helper_sha256` field or child helper mount is added. The
original anchor/context checks and startup ownership remain in force. Both
backends consume the same original 7800-second allocation and the second starts
only after the first's positive owned cleanup.

| Bound | Original D28 | Semantic140 |
| --- | --- | --- |
| Callers / sequential requests each | 50 / 4 | 140 / 4 |
| Requests per backend | 200 | 560 |
| Sources per backend | 25 + 25 | 25 + 25 |
| Preparation physical cap / per space / active | 450 / 225 / 2 | unchanged |
| Measurement physical cap / per space / active | 600 / 300 / 50 | 1680 / 840 / 140 |
| Whole physical reservation ceiling | 2100 | 4260 |
| Spending cap, only after separate allocation | $3 | $5 |
| Gateway transport slots | 64 | 160 |
| Global gateway capture ceiling | 20 GiB | 40 GiB |
| Per-export channel bytes / indexed files | 20 GiB / 10532 | unchanged |
| Per-request capture aggregate / backend | 128 MiB | 512 MiB |
| Combined measured observation / backend | original behavior | 512 MiB |
| Runtime-original pool / backend | 352 MiB | 2 GiB |

The source SDK permits three total sends per embedding batch. Preparation's
ordinary worker capacity remains three attempts. The arithmetic is
`2 * (50 * 3 * 3 + 560 * 3) = 4260`. Preparation batch splitting or recovery may
consume more than ordinary arithmetic; every such reservation must still hit
the same hard caps. No successful completion is promised for every recovery
path. At the original frozen rate each attempt reserves 1,064,960 nanoUSD:
4260 reservations total $4.5367296. This is inherited source arithmetic, not a
current pricing claim or permission to spend. Applicable terms must be verified
before execution. Failed or uncertain sends do not release reservations.

Wire limits and original acceptance thresholds stay fixed: semantic search,
50 complete results, 262144 response bytes, 8 seconds per request, a 120-second
measurement window, p95 strictly below 2 seconds and p99 strictly below 5
seconds, with nearest ranks 532 and 555 over all 560 requests. Every returned
result retains the original evidence/security checks; required evidence must
appear in the first three distinct sources and mean reciprocal rank must be at
least 0.90. No warmups, hidden retries, source injection or lexical replacement
may qualify.

The independent successor oracle requires finite nonboolean timestamps for
every row, ordered as `started <= connected <= write complete <= first response
byte <= ended <= caller finished`, with a positive sent interval. It rejects
top-level capture, cancellation and cleanup errors even if a parsed body is
complete. It recomputes both request-lifetime and sent-waiting peaks as exactly
140, ending intervals before starting others on ties. Ready tasks alone cannot
substitute for measured overlap. These client intervals do not prove concurrent
server computation. Missing overlap is an unmet load obligation, not permission
for artificial delay or another allocation.

The four full cohort windows total 7440 seconds; all worst-case begins and
closes would raise that to 8000 seconds before other work. The retained 7800
seconds is an absolute truncating ceiling. Startup, preflight, exports and
cleanup consume it, and no phase or backend renews it. An 8-second public
timeout does not establish cancellation of a provider send; unresolved sends
remain active under their original deadlines and can cause honest later refusal.

Capture limits refuse before oversized writes; evidence truncation cannot
qualify. The successor retains original per-request files as well as the
combined observation and counts distinct physical paths even when their hashes
match. Gateway accounting remains lazy and per export. The proposed 44 GiB
whole capture ceiling plus 1 GiB headroom is subordinate to the actual remaining
shared 200 GiB machine allowance and 250 GiB free floor. Disk limits do not
prove memory bounds for parsed JSON. Indexed payload entry counts exclude the
index file itself, ledger/seals and other runtime files; actual physical byte
accounting must include those copies. No ledger reset or resource permission is
provided by this profile.

The new independent `semantic140_expectations.py` and producer
`workload_profiles.py` must be included in exact source/helper inventories.
The orchestrator loads the reviewed expectations snapshot into its temporary
module namespace before importing validators, then restores previous modules
on success, refusal or cancellation. The producer helper has a separate source
hash pin; it supplies routing and bounds, never independent known-answer truth.

G's additional offline control sources are `semantic140_driver_test.py` and
`scripts/qualification/semantic140_bootstrap_test.py`. They cover preservation,
closed input identity, malicious corpus changes, 140-by-four scheduling, quota
spillover, duplicate physical capture bytes, startup refusal, original clock
reuse and cleanup. Original driver/preparation/orchestration/startup controls
remain required alongside S's oracle controls and B's gateway controls. Source
availability or static checking does not claim those tests have executed.
Legacy original-file reads retain their existing call contract and implicit
legacy byte bounds. Only successor reads pass an explicit successor profile;
both fixed gateway input mounts retain the same 128 KiB and private-file checks.

This profile covers the approximately 140-search portion of the original
OB3-shaped workload requirement. The consumer-owned outage twin and review
batch, degradation counts, large/shared inputs, no-op/revision/retraction and
security phases retain their separate contracts. OB3's real-data gate remains
physical source deletion plus bounded backup retention; restore replay is
recommended to OB3, not its gate. Recall's durable journal and restore barrier
remain required. HomeCore's independent acceptance and optional standalone
Recall behavior are unchanged.
