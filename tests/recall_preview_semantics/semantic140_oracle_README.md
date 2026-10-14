# Independent semantic140 qualification expectations

`semantic140-v1` is an additive closed workload profile. The original D28 corpus,
contract, preparation files, allocation and 50-caller evaluator remain available.
The new profile selects six exact reviewed input hashes. Unknown profile IDs,
old/new input mixtures, and arbitrary caller-provided quotas cannot select it.
`semantic140_expectations.py` holds the independent literals; it does not import
collector or gateway policy implementations. Legacy paths never import or read
this new helper. Successor validators and the workload oracle read its exact
SHA-256-pinned source snapshot and execute those verified bytes, without cached
modules, ambient import paths or bytecode files. New static reviews must include
the same helper hash in their reviewed inventory; historical inventories remain
valid for their original profile.

The active `workload.evaluate_cohort` entry point requires all 560 scheduled
attempts and 140 caller identities. It preserves every original query, source and
known-answer definition. All 100 intents are reported individually, alongside the
frozen weighted per-request MRR: 80 intents occur six times and 20 occur four
times. Every returned result goes through the existing exact source/revision,
quote, byte-span, named-space and complete-coverage checks. Required evidence
remains in the first three distinct sources; minimum precision is unchanged at
one third, and mean reciprocal rank must remain at least 0.90.

Every caller must reach one common barrier and issue its four requests in order.
Each request must retain finite, non-boolean timestamps in this order:

```
started <= connected <= write_complete <= first_response_byte <= ended <= caller_finished
```

The write-complete-to-ended interval must be positive. The oracle independently
recomputes request-lifetime and sent-waiting peaks using half-open intervals:
ends precede starts at an equal timestamp. Both peaks must be exactly 140. Ready
tasks, analysis workers and reported peaks alone cannot satisfy this predicate.
These are client observations; simultaneous server computation remains unknown.
A peak of 139 is retained as an unmet obligation, without manufactured delays or
extra requests.

Complete response bytes do not excuse a top-level capture, cleanup or deadline
error. Failed or missing attempts cannot disappear from the 560-attempt cohort.
No retry, source injection or fallback is accepted. Each request remains bounded
by eight seconds; the common measurement ends within 120 seconds. The original
strict nearest-rank cutoffs apply to all attempts: rank 532 must be below two
seconds and rank 555 below five seconds. The original response bound remains
262,144 bytes. JSON sizes are counted incrementally against separate 512 MiB
measurement-row and observation ceilings, with refusal rather than truncation.

The dynamic component validator binds the explicit new review, v2 launch anchor
and phase profile to the exact allocation and all six original input files. V2
adds only `profile_id`; it introduces no profile-helper field or runtime import
authority. The old v1 anchor shapes remain exact. The new runtime raw-evidence
pool is 2 GiB; preparation, installed capture and per-provider-export limits
remain unchanged.

The preparation-provider validator independently enforces the 4,260-attempt and
$5 whole-allocation ceilings, without refunds, with the same original 7,800-second
clock. Before second-backend preparation, its settled first-backend prefix may
contain at most 2,130 attempts and 1,065 per space. Preparation remains capped at
450 attempts/backend, 225/space and two active physical sends. These are hard
limits, not guaranteed sufficient budgets for recovery or recursive batch splits.
The gateway owns physical measurement admission at 1,680/backend, 840/space and
140 active sends; source-selected gateway/measurement evidence remains required.
A public timeout does not establish cancellation of an upstream send.

The 40 GiB whole-allocation capture policy does not enlarge any accounting export:
each export remains bounded to 20 GiB and 10,532 files, with 8 MiB records and a
256 MiB lazy cache. The independent validator keeps original raw hashes, complete
closed-cohort membership, source/input bindings and unsettled-send refusals.
Provider totals do not establish per-public-request attribution.

The two `semantic140_*_test.py` files contain synthetic control transcripts and
known-answer responses. They test the oracle and evidence boundary, not model
quality, real request overlap, installed behavior or consumer adoption. Their
source checkpoint records whether they have actually executed. The OB3 consumer
review batch and real Recall-outage twin remain separate acceptance obligations.
