# Independent OB3 outage receipt oracle

`scripts/qualification/ob3_outage_oracle.py` exposes the pure function
`evaluate_receipt_bytes(raw_receipt, raw_expectations)`. It performs no I/O,
process, product, network or provider operation. Both arguments are original
bounded JSON bytes; their SHA-256 hashes are retained without re-encoding.

New receipt acceptance requires `ob3-recall-outage-2`, read from original OB3
`d2e45817117d67d23bcdc8e33a021b6c0c56f184` source and schema relayed by root.
All nine producer check fields are required, including `cross_total`,
`participants_match_reads`, and `no_duplicate_participants`; their boolean
values still cannot establish independent success. Original v1 receipt bytes
remain historical and return `unmet` at this new entrypoint.

The original source review of `b789af13` and its frozen v1 schema
found three gaps in the producer's then-current pure verdict: it balanced total
and worker counters separately without comparing their sums, trusted declared
participant lists without equating them to positive worker reads, and silently
deduplicated participant arrays. The v2 producer addresses these with checked
arithmetic and raw-list checks. The independent oracle still recomputes all
three against its original expected set and baseline, independently of producer
flags. The old producer source and v1 schema remain unchanged historical evidence.

The caller must supply independently reviewed original expectation bytes with
exact fields:

- `format`: `recall-ob3-outage-expectations/v1`.
- `run_id`, full `ob3_git_sha`, `recall_contract_version`,
  `recall_endpoint_state`, and `batch_kind` from the original recipe/controller.
- `requested_worker_ids` and the independently declared nonempty
  `expected_participating_workers` subset. The candidate cannot choose either.
- `por_baseline`: all four original failed/degraded/fetched/inserted counters.
- `maximum_logical_reads` and original RFC3339 `earliest_started_at` /
  `latest_finished_at` bounds. A fresh receipt cannot reset those limits.

Unknown fields/reasons, duplicate JSON keys or identities, missing batch or
baseline, malformed counts (including booleans), and unsupported shapes return
`unmet`. Established identity, interval, accounting, participant, completion,
outage-evidence or baseline violations return `fail`. The original producer
verdict is recorded but never used as success authority.

Every reported worker must belong to the requested set. Every expected
participant must have positive reads, and all positive-read workers must be
exactly that set. Every worker and the total must balance, and each total count
and each fallback reason must equal the sum of the original worker fields.
Configured outage participation cannot be satisfied by `no_expected_sources`.
All requested workers must complete with zero failures. Both candidate baseline
and with-Recall POR counts must equal the independently supplied original
baseline; changing both candidate copies together cannot pass.

Sparse per-worker fallback maps are part of the actual producer wire. The total
contains all nine frozen reasons, including refused and transport. Unavailable
is an actual supported endpoint-state enum value in the producer source.
`size_retries` exists only at aggregate level in this wire: the oracle checks
its bound against logical reads but cannot prove per-read retry history from
absent per-worker fields. Original call traces remain necessary for that gate.

The result scope is only `external_receipt_consistency`, always with
`product_acceptance: false`. Neither input proves its own provenance. The caller
must bind source, controller, baseline and actual capture custody independently.
Receipt counters alone do not establish physical network attempts, installed
artifacts, process cleanup, an external twin run, or production acceptance.

Fifteen pure unittest controls are authored beside the helper, covering the
three concrete verdict defects plus original identity/baseline/interval,
malformed evidence, missing batch, vacuous producer PASS and byte custody.
At this checkpoint only AST parsing and diff checks were performed; these
controls have not been executed. Root owns later bounded validation scheduling.
