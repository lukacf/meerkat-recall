# Selected full-workload source review

This review reads the actual A driver at
`dbdb897930ef92bdffbffabb6f6d537a0f92d282`, the seven component pins and assembly
at I `ca15d8af10c02ec1762cdac93fa18ed87aaad2b5`, frozen D inputs, and E's final
manifest at `b39341b7fc087e1912906c200b2d6e177acf90eb`.
The companion JSON records exact source identities, literal checks and remaining
findings. It is an independent read of A/G/S methods by I. I owns the assembly
and candidate expectations, so the assembly checks are producer consistency
checks, never independent qualification or approval of that candidate plan.

## Selected implementation findings

- `query_evidence` requires exactly nine citation fields. `evidence_coordinates`
  compares canonical `doc_id` with both the original accepted document and the
  actual immutable revision's parent. It checks the original operation/head,
  source digest and provenance, immutable full-text digest, both UTF-8 intervals,
  rendered support digest, and the minimal positional raw cover under the
  versioned renderer. It does not select another equal quote occurrence.
- FW09 first observes the actual failed-retryable operation, then samples the
  current running, pending and failed-retryable operations. Original queries and
  bounded intake occur while the running extraction is held. The failed source
  is retracted with its observed original generation before releasing the hold.
  FW11 captures the held 503, revokes current provider permission, preserves the
  observed failure category, and retracts that original before restoring the
  binding. Both retain the historical terminal receipt separately from the new
  retraction receipt/head. Per-rule physical-send counts remain fenced in
  subsequent driver checks. Failure cleanup may release an owned hold, but then
  dependent cases stop; that path does not report successful qualification.
- FW12 passes the original 280-entry expectation map and its separate hash,
  unchanged, to G's load helper with `Driver.query_evidence` as the synchronous
  callback. Every successful response must complete that same validation. Each
  original request has nonempty required evidence; 48 requests additionally
  require evidence on an actual claim. Empty results, foreign evidence or
  mismatched document/revision/coordinates cannot satisfy these requirements.
- The load helper requires all 140 callers ready before dispatch, two sequential
  requests each, no warmups/retries, exactly 280 physical HTTP calls and unchanged
  provider counts. Its reported intervals are explicitly client intervals, not
  server execution overlap or semantic latency qualification.
- Original acceptance is captured before sending and uncertain requests are not
  automatically retried. Replacement recipes bind an observed original head
  generation; the competing-revision case sends two distinct revisions against
  that same generation and expects one typed conflict without a CAS retry.

## Original literal and source consistency

Without importing or executing workload modules, a separate standard-library
read checked all seven committed component blobs and five pinned input files.
All 28 Graph recipe files and both spy files match E's final raw/canonical hashes.
The selected 24 workload recipes retain the 50 original baseline identities plus
six declared extras, for 56 total, with both named policy-3/policy-7 envelopes.
Both large Graph inputs retain exactly 2,000,000 UTF-8 bytes and D's original
digest; the additional-byte refusal digest also matches.

The exact 280 request IDs, caller/ordinal pairs, per-space distribution, revision
constraints, coverage literals, full quotes and quote digests were checked
against original D corpus and E recipe bytes. The map hash remains
`09ba111bcdcc8f9d91c6b2df44c2d17c619f45c76b039e5bc17797f49826d5d4`.
Every required claim quote exists in the independently authored assertion set
for that same original source and revision. Controlled limits match the frozen
D contract exactly. The candidate file hash remains
`438476e49d7ef1ff1db079d7049f8c2ba8a76279210b2cf3fc50c037f8c3cdc0`.

## Remaining source and execution limits

**I-FW-01: code-byte custody remains open.** The selected CLI imports helpers
before separately hashing files, and the driver's OB3 adapter later reopens its
oracle with `SourceFileLoader`. Seven file hashes therefore do not alone bind the
exact executed module bytes. A/S own a bounded correction using the original
verified code bytes, with no later filesystem/bytecode read replacing them.

**I-FW-02: actual effective worker recipe is still required.** The CLI checks
startup artifact/nonce/backend and `workers_enabled`; it does not validate the
effective concurrency, retry and embedding recipe needed by the approved FW09
control direction. Actual process evidence and independently specified expected
values must close this activation prerequisite. Current-state sampling remains
valuable but does not manufacture missing process configuration evidence.

**I-FW-03: external owner seams remain explicitly unmet.** The CLI supplies
neither export/compact capacity capture nor service-stop control. The driver
records these missing observations and leaves consumer physical fallback/outage
acceptance unmet even if the original OB3 receipt consistency oracle passes.
Those truthful limits must remain until actual owner captures exist.

**I-FW-04: coverage-key review remains with D/G.** The load predicate checks every
expected coverage entry but does not reject additional keys. All returned
evidence still receives the same strict source checks. This review does not
treat that predicate as proof of absence of extraneous coverage keys; D must
decide exact-key enforcement against the scoped-response contract.

D still owns final candidate configuration/query/cleanup/load approval and
whole qualification. Controlled model responses do not prove model quality,
private manifests, physical erasure, backup retention or consumer completion.

## Validation performed

Read-only Git/file/AST/JSON/hash and literal arithmetic checks passed. The exact
driver test source contains 32 test methods. None was executed by this review;
central authorization and execution remain separate. No helper imports,
workload execution, sockets, providers, service starts, Cargo or Rust execution
were used. Direct `git diff --check` is the source-document validation.
