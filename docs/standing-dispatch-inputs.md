# Original standing authority at physical dispatch

`DurableUsageObserver::before_dispatch` checks its original live attempt and then
calls `reprocess::guard_physical_dispatch`. That consumer captures the canonical
epoch before reading the original Work authority. Recovery owns the single
private execution-ancestry resolver; maintenance does not reinterpret standing
delegations. Every applicable original principal authorizes every actual input
through intake's `authorize_standing_source_input` mapping. Source uses the
existing source read/write capabilities; Doc, DocRevision, DocNode and WorkArtifact
use the existing document capabilities. Missing history does not become an
independent root. A genuinely separately admitted root retains its own authority
and budget instead of inheriting an unrelated historical content producer.

The outer Work observer preserves the original epoch's explicit presence or
absence using `observe_epoch`, then compares that same observation at the end.
A valid standalone Direct Work can precede any canonical-domain row; absence
does not mean missing execution authority. A foreign or malformed epoch row
refuses, and an absent epoch becoming present invalidates the original capture.
No read initializes it. Actual Source input captures continue to require a
present epoch through `current_epoch`. Modern SpaceConfig and ReprocessJob
constructors already initialize epochs through the existing writer fences.

`PreparedStandingDispatchInputs` is an ephemeral read-only capture, implemented
as a child of `source_document_guards` and re-exported by storage. It calls the
same `current_watch_document_guards` selector as intake. It preserves actual
Source/head presence or absence, current desired-template/manifest Docs, expected
absent physical Doc IDs, original manifest presence/content and the epoch from
before discovery. It never scans unrelated historical Docs. Finalization's
`declared_input_records` supplies the exact original manifest's required
revisions, representative/coalesced/embedding nodes, metadata/planning artifacts,
and materialized declared knowledge bundles and their actual candidate input.
No arbitrary prefix or ancestry-derived record cap truncates the admitted input
population. This is the desired manifest's declared population; provider phases
before manifest creation require their actual input coverage to be checked at
the dispatcher integration boundary.

Absent head and manifest observations also check the physical key globally,
without a space filter. A foreign control at that key invalidates absence.
These point reads and the epoch read are read-only and cannot create a managed
checkpoint. The current generic Work observer checks the whole declared input
population per call; many calls over one large graph can therefore repeat linear
work. This preserves complete coverage without claiming a per-call performance
optimization. The pre-manifest summary and media consumers must separately
retain their actual input rows and original execution authority at physical
dispatch; those integrations remain prerequisites for the complete standing
authority group.

The consumer retains those original rows in the shared standing proof, executes
the original finite-budget checks, then verifies every original input capture,
the same standing proof, and the original epoch. It never refreshes a grant,
claim, source generation or epoch to make a failed precondition pass. The
canonical owner separately uses the capture's `document_guards()` accessor and
the standing proof's retained rows inside the same transaction as effects. A
proposed after-image only receives the shared pure policy check; it is never
inserted into the original read set.

The accounting walk preserves the same ancestry bound as recovery: 256 distinct
Work nodes and 257 Source-operation nodes, with active-path cycle refusal and
successful completed-node deduplication across branches. Failed verification
never marks a node complete. Source intermediates do not consume the Work-node
limit. Input-record population is a separate concern, bounded by the actual
admitted manifest and its declared candidate references.

Source tests create actual signed admission, stored delegation, atomic Watch
queue entries, original claims/runs and private successors. They cover completion
of the original producer, current Source policy denial, grant revocation, a new
independent root, and stale original Work refusal. Additional tests capture a
real absent head, admit a source, observe its absent Doc, stage that actual
accepted input, and reject stale captures and changed Doc policy. Traversal tests
cover the full interleaved boundary, convergence and cycles. No test fabricates a
source completion, standing descriptor or canonical publication receipt.
Standalone Direct Work tests create actual original private origins and claims,
then require repeated dispatch preflight to succeed without creating an epoch;
they do not claim provider approval for an unconfigured space.

The retained source-operation budget oracle also registers its original Watch
through the actual atomic registration API, with an explicit bounded local
principal. Its real private successor carries the returned delegation identity.
It continues to test sticky ancestor currency restrictions after the original
Watch claim completes and after mutable scheduling observations change.

These tests are authored source. No local Cargo/build/test execution has been
performed; qualification and central runtime scheduling remain with integration.
