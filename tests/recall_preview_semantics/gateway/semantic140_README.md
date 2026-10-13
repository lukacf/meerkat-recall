# Closed semantic140 gateway profile

The separate `semantic140-v1` profile accepts only the pinned successor
allocation, contract, corpus and typed preparation documents. It is selected by
`recall-semantic140-embedding-manifest/v1` and its required `profile_id`, never by
user supplied limits. Its accounting mode is
`separate-exclusive-semantic140-embedding-reservation/v1`. A reviewed manifest
and current approval remain necessary; the checked-in data grant no execution.
`manifest.semantic140.example.json` is deliberately inactive and contains no
actual account, credential, runtime or approval facts.

The original primary 432-attempt allocation and D28 2100-attempt, $3, 50-caller
allocation retain their formats, constants and default behavior. The successor
uses a new ledger identity and cannot resume either original accounting mode.
It reserves at most 4260 physical attempts, $5 and 40 GiB of gateway captures.
Each reservation permanently consumes 1,064,960 nanoUSD, including failed and
uncertain attempts. There is no success refund, ledger reset or scope reduction.

Both backends retain exclusive preparation then measurement cohorts. Preparation
remains 450 attempts, 225 per space, two unresolved sends and 3600 seconds.
Measurement allows 1680 attempts, 840 per space, 140 unresolved sends and 120
seconds. The original allocation clock remains 7800 seconds, measured using
both wall and monotonic clocks and the same boot identity. Phases never renew
that interval. Ordinary retry arithmetic is capacity, not a success guarantee:
recovery and multi-input splitting must consume the same hard ceilings.

The host gateway and loopback bridge use 160 transport slots and backlog only
for the successor. The bridge derives these values from a validated v2 launch
anchor and rejects environment overrides. A v2 anchor adds only the exact
`profile_id` to the original fields and binds the exact successor allocation,
corpus and caps. Its validator stays pure; the original pinned context-helper
and bridge sources cover the new logic. No additional child helper or mount is
introduced. Original primary/D28 v1 anchors keep their original field sets.

Control requests include `profile_id` only for the successor. Missing or mixed
profiles fail before reservation. Closure remains available after approval
expiry or revocation: it first closes admission durably, then reports unsettled
sends without fabricating completion. Replay requires the same observation
hash, and the next phase cannot begin until closure completes. An eight-second
public timeout does not imply upstream termination; unresolved sends still
occupy physical slots until the gateway records their outcome.

Accounting export remains bounded per cohort by 20 GiB, 10532 indexed entries,
8 MiB records, 4 MiB receipts and the existing lazy cache. A 1680-attempt export
fits those bounds; the 40 GiB global capture budget does not widen an export.
Indexed payload counts exclude each export index itself and other filesystem
metadata. Per-public-request provider attribution remains unmet.

`test_semantic140.py` covers profile/anchor mixing, legacy limits, host and bridge
capacity, unresolved-send accounting, per-space and global exhaustion, durable
close-before-capture, unchanged export/replay, phase and whole clocks, resume
refusal, and revocation-safe closure. Positive capture tests use real SQLite
and file sealing with synthetic transport. Allocator-history fixtures are
explicitly unsuitable as provider qualification evidence. Tests block socket
and process creation and do not read credentials. These tests require the
coordinated six successor data files from the grounding owner before execution.
