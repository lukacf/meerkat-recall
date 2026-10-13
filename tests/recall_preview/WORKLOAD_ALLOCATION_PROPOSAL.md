# Separate D28 prequalification provider proposal

The [machine-readable proposal](workload-allocation-proposal.json) is frozen for
source implementation and review. It authorizes no execution, provider access or
spending. The original semantic432-attempt proposal and gateway delta retain
their exact original byte hashes. G owns the separate exclusive-cohort accounting
mode and bridge; S owns the public preparation helper and read-only load collector.

The explicit [typed-envelope delta](workload-allocation-typed-delta.json) supersedes
the original preparation representation, with no budget or corpus change. Its
[preparation contract](workload-preparation-contract.json) fixes both record
envelopes at household-policy/3 and business-policy/7, empty labels/handling and
subjects, Unknown subject state, and empty input provenance. G/S must bind both
the original allocation and this delta before a run can qualify.

| Phase | Original public calls per backend | Physical embedding stop ceiling per backend | Active physical limit |
| --- | --- | --- | --- |
| Preparation |50 Retrieval source accepts,25 per space |450,225 per space |2 |
| Measurement |200 searches,100 per space |600,300 per space |50 |

The two backends run sequentially with preparation closed before measurement.
All four cohorts are single-use under one original durable allocation. They
share a **2100 physical-attempt ceiling, $3 cap and130-minute lifetime**. Every
embedding attempt permanently reserves1,064,960nanoUSD, conservatively charging
8192 total input tokens at the large model's standard $0.13/million rate. The
maximum reservation is **$2.236416**. Rates and applicable account terms must be
rechecked before a later live allocation. There are no generation roles or
embedding refunds in this proposal.

The retained typed `POST /v1/sources/ingest` accepts explicit Source+Doc records,
lowercase `ingest_mode: retrieval`, title/text, stable rendition key and nested
original intake identifiers. Normalized input defaults to level 0 and therefore
cannot prepare this corpus's classification. The typed path runs actual source acceptance, deterministic
triage, rendering, tree creation, real embeddings and whole-source finalization.
It does not exercise Graph extraction, support verification or wiki semantics;
those unchanged gates remain separate. A202 or empty queue cannot qualify setup.
Caller UUIDs are input placeholders: the server resolves external source identity
and may assign a different immutable Doc ID. Oracles bind the actual returned
Source/operation/head/Doc/revision identities and exact original corpus bytes;
they do not reproduce private ID derivation or require output IDs to equal inputs.

The installed OpenAI embedding code permits three total physical sends per
logical batch. Pinning `PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS=3` permits
ordinary handled-failure work attempts1,2,3. The setup capacity is therefore
50×3×3=450 per backend; measurement capacity is200×3=600. The collector never
retries its200 public requests or inserts warmups. Recovery, refunded blocked
attempts and repaired work can exceed the ordinary worker arithmetic. They still
consume the original gateway ceiling; exhaustion leaves qualification incomplete.
This is a finite stop limit, not a promise that every failure sequence completes.

G checks the actual predispatch embedding body:1–64 input texts and8192 total
UTF-8 bytes as a conservative token upper bound, exact permitted space/profile,
model and dimensions. Unpublished nodes are not exposed merely for this test.
The setup receipt must separately establish the actual original accepted
operation, immutable revision, published node inventory and complete index
coverage through retained evidence. A guessed two-node tree is insufficient.
The short corpus is expected to produce paragraph/chunk nodes in one batch;
that expectation cannot increase the gateway's hard cap if the implementation
produces something different.

Each preparation cohort lasts at most3600seconds; each measurement cohort lasts
120seconds. Provider attempts use the minimum of120seconds and the original
remaining cohort/allocation deadline. Closing a cohort blocks new admission;
unfinished sends retain their reservations and cannot acquire a new budget on
reopen. Exact cohort dispatch totals can be proved. Association between a public
query and individual provider attempts remains explicitly unmet.

The retained wire ceilings are131072bytes each for original and forwarded
requests and8MiB per provider response. A32768-byte metadata allowance gives
18,235,392,000bytes for2100 attempts; G reserves20GiB including bounded ledger and
control receipts. The implementation must enforce those bounds and the original
250GiB host free-space floor. These are proposed capture limits, not allocated
disk or a started service. The independent collector's262144-byte public response
ceiling remains unchanged.

Root, C, S, B and G supplied source-contract reviews. Concrete gateway/helper
code, original ledger custody, exact installed artifact/configuration/account
identities, and actual preparation/load results are still required. The larger
approximately140-search stress,2MB/shared payloads, no-op/revision/retract cases,
permission and outage negatives, and consumer20-to10 fallback require separate
explicit inventories and budgets. They are not hidden inside these200 successes.
