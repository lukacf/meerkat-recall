# D28 independent workload contract

This separately frozen workload contains **50 total sources**, 25 per named
space, 100 original query variants and 200 scheduled requests per backend.
It leaves both primary semantic corpora and their scoring rules unchanged.
Source fixtures repeat external keys and revision labels across spaces while
keeping their bytes and facts different; cross-space evidence cannot pass.
Distinct approved profiles are small/1536 and large/3072. Actual account,
credential generation, configuration, artifact and gateway custody are required
in the later run allocation; their presence is not established by this fixture.

Root froze 50 callers, four sequential requests each, a common readiness barrier,
120 seconds total, eight seconds per request, and strict p95 < 2000 ms and
p99 < 5000 ms. Actual physical request overlap is computed from complete request
intervals and reported separately. OB3's 140 analyses over 51 minutes, peak 50
running analysis workers and approximate mean 34 are not Recall overlap data.
This run is prequalification. The original approximately 140-search stress and
large/shared-input, no-op, revision, retraction, permission and outage workloads
remain separate obligations with explicit later schedules and allocations.

Every scheduled request uses its original question, semantic ranking, wire limit
50, exact revision constraints and a 262144-byte complete JSON response bound.
OB3 accepted that byte bound. Its consumer default20/one size-error retry at10/
fallback policy is a separate consumer case; no retry is hidden in these 200
measurements. Negative, retraction and outage phases require explicit separate
inventories and physical attempt counts. No provider budget is authorized here;
the original 432-attempt semantic proposal does not include additional load calls.
The separate [allocation proposal](WORKLOAD_ALLOCATION_PROPOSAL.md) now freezes
2100 physical embedding attempts/$3 for source implementation, including ordinary
SDK/worker retry capacity. It remains inactive pending concrete implementation,
artifact custody and later spending authorization.
The separately pinned typed preparation contract preserves the corpus's exact
policy/level envelopes; normalized level-0 intake cannot satisfy this setup.

The metric cutoff remains three distinct source/revision pairs, in first wire
appearance order after collapsing duplicate evidence for the same source.
Every returned hit and evidence span, including those after the metric cutoff,
must have known source bytes, exact UTF-8 offsets/quotes, current complete
coverage and allowed space/revision. Required source evidence must occur in the
first three and contain the independently frozen answer fragment. Precision
uses denominator3; cohort mean reciprocal rank must be at least0.90. Returning
50 hits never changes the denominator or rescues evidence ranked fourth.

`workload.py` has no network or mutation capability. `workload_response(query,
response, corpus=None)` validates retained raw response bytes and returns scores;
`evaluate_cohort(observation)` recomputes all200 correctness and latency results.
Inputs include the exact contract/corpus hashes, backend, source counts,
`barrier_released_ms`, 50 caller `{id, ready_ms, finished_ms}` records and every
scheduled request with original `request_body`, start/end times and full capture
under `response`. It rejects omitted/extra calls, overlapping sequential calls,
changed scope/query/mode, failed bodies and claimed overlap inconsistent with
intervals. Timing is milliseconds from one cohort's monotonic origin.

These evaluators cannot authenticate a dishonest capture producer. The driver
must retain source inventory, actual completed publication/profile/artifact
receipts and original raw transport captures. Public search responses do not
provide provider attempt IDs; correlation requires separate qualified gateway
run/cohort captures. The driver belongs to S, the expected corpus/verdicts to D.
Twelve offline evaluator tests are authored; they are not a measured load run.
