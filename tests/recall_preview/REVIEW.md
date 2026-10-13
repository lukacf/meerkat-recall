# Independent source review checkpoint

This is static review, not executed acceptance. Initial D checkout:
`f59e6f5c99ed095a015656626f4d32c106c13075`. No production action, provider probe,
Cargo invocation or failure reproduction supports this report.

Later source review also identified these active-path gaps:

- **Q08:** The knowledge-view outbox loop held restore activity only during its
  effect callback, then released it before advancing the cursor. A restore could
  replace those effects between callback success and acknowledgment. Cursor load,
  event fetch, effects and CAS advance must share one restore admission scope.
  Recovery confirmed the finding and is routing both real consumers through a
  shared `outbox_delivery::deliver_batch` helper; authored source is not runtime
  evidence. The old repository unit test which simply omitted `advance` on a
  supposed failure did not exercise the actual consumer behavior.
- **Q09:** `source_decision::inverse_effect` compared nested metadata as one
  top-level value. After B changed `context.value`, an independent edit to
  `context.independent` made the whole object differ, so neither nested value was
  reversed. Erasure must remove B's contribution and preserve the independent
  edit. The new active erasure target drives that exact canonical decision chain;
  it must not be weakened to a same-field conflict or a source-only deletion.

Finalization follow-up `225ef27` preserves accepted Source external identity,
type, hash and security while retaining normalized aliases in metadata. That
addresses D's source identity review question at source. It also prepares prior
source retirement for metadata/zero-group completion, so omitted graph groups
cannot bypass replacement cleanup. Execution remains unqualified.

| Source | Disposition |
| --- | --- |
| Original F01 tree `bb4d3d563301f489871825daf1097b20ff4e3d87`, recovered as `2b22dc6eac5f98bb23bec197099df7df011ca3a7` | Retain record checks, redaction, guarded relationship/retry writes and read-only middleware. Original checkpoint failed compilation before tests. Exact provenance and source lifecycle require further work. |
| Surface `985ec319560d9e7d73a3fd8f7b95ccee3d260577` | Bounded delta fixes the missing revision trait while preserving raw subjects/completeness, checks chunk lineage and places space read-scope preflight before existence lookup. No additional defect identified in that narrow delta. |
| Packaging `cb9a598ff214670f8b894d8b9b1b7c0399515b59` | Sibling inode lock and path canonicalization are useful. Ownership lifetime must also cover escaped underlying backend handles; see below. |
| Packaging `f6ac6972a4ee3e9031a86389e0ce43e1fd255dca` | Source repair keeps the guard on escaped EmbeddedStore handles and removes the raw Surreal handle getter. Q03 is repaired at source; new regression remains unexecuted. Two existing test harnesses still require adapting the removed getter. |

## Findings sent to implementation owners

**Q01: exact provenance is not established by authorization alone.** Original
F01 `GetProvenanceHandler` checks record envelopes, revision identity and node
parentage. It still constructs a quote from `EvidenceRef.span` without comparing
the full span/hash/node to `assertion.evidence` or recomputing the hash. Invalid
ranges can omit the quote while the enclosing chain succeeds. The referenced
Source is not resolved for current lifecycle. This is residual integrity work,
not a claim that token verification or the new per-record checks are absent.
Surface accepted the finding for its next checkpoint; retrieval now owns the
active read paths. Require exact immutable evidence or an explicit unavailable/
invalid-evidence outcome, including retained direct-write paths.

**Q02: immutable revision creation drops subject lineage at the reviewed base.**
`execute_render_doc` copies Security/provenance into revision BaseFields but does
not copy `subjects` or raw `subjects_state`. The new trait faithfully exposing
default Unknown/empty state cannot restore omitted creation-time information.
Grounding owns the production propagation fix. The independent positive test
requires the full classified lineage, not just level or policy ID.

**Q03: writer guard lifetime is shorter than exposed backend lifetime.** In
`cb9a598`, `_store_writer` belongs to `StorageClient`. Its public
`embedded_store()` returns an independently owned `Arc<EmbeddedStore>`, and
`db()` exposes a clonable Surreal handle. Those handles can outlive all clients,
so the old backend remains usable after the guard is dropped. Bind ownership to
the actual shared backend lifetime or prevent such handles escaping. Ordinary
client-clone unit tests do not cover that case. Packaging was notified; follow-on
source review and executed regression evidence are still needed.

The `f6ac697` follow-up addresses Q03 at source by placing the guard on the
shared EmbeddedStore and removing `StorageClient::db()`. Independent static
re-review found that the two existing scenario harness constructors still call
the removed getter; their unused raw-db fields should be migrated by the owner.
No lock test or installed artifact was executed by D.

**Q04: canonical replay needs a semantic plan fingerprint.** The first authored
`canonical_commit.rs` implementation compares only space/commit/source/revision
identity in `check_replayed_commit`. Changed effects or outbox contents under the
same commit identity can therefore return an old successful receipt. Compare a
persisted canonical semantic fingerprint, not just identity or existence. D's
new actual-transaction regression reuses a completed commit identity after
changing an entity label and requires an explicit conflict. Source review and
test authorship do not establish whether the eventual joined implementation
passes.

Static follow-up: integrity's subsequent `canonical_commit.rs` now compares both
`input_fingerprint` and `plan_fingerprint` on receipt replay. The authored changed
entity-label counterexample is retained; source inspection is not an executed
passing result.

**Q05: erase intent sequencing must bind to accepted authority.** An external
journal append after a read-only precondition check but before a later source
CAS would leave a durable erase intent even if a newer source acceptance made
that CAS fail. The implementation must fence accepted erasure authority durably,
append the independent journal before physical removal, and recover every crash
boundary. Corrected content requires a fresh source incarnation so replay of an
old intent cannot erase it. This requirement was sent to integrity/durability
while their active call sequence was still being written; it is a design gate,
not a claimed reproduced defect in a finished erase endpoint.

## Publication design review requirements

**Q06: maintenance must bind the exact published rendered revision.** At the
reviewed maintenance source checkpoint, accepted-head evidence compared a
`source_content` marker with accepted source hash, and revision text with its own
hash. It did not consult immutable document/revision mapping or compare text/plain
bytes with accepted input. Self-consistent changed evidence can retain an old
marker; equal content hashes also do not identify equal revision semantics.
Maintenance/retrieval owners were asked to share the exact mapping/input check,
while preserving old published evidence during a pending replacement. This is a
source counterexample, not an executed reproduction.

**Q07: MCP policy failures must remain nonretryable authorization errors.** The
reviewed scoped handler's preflight returned `PolicyError`, while HTTP MCP's
generic error branch converted it to retryable `unavailable`. Its shared search
function already projected the same check to `forbidden_space`. Surface/retrieval
owners were asked to reconcile retained and new handlers centrally. Recovery
barrier refusals also need their explicit `not_ready` classification. Safe generic
messages must not erase the consumer's retry semantics. No transport probe ran.

Detached planning followed by a changed-row diff does not establish serializable
publication. Validate all decision reads in the canonical transaction, including
absence, identity/slot membership and unchanged referenced records. A global
snapshot generation is sufficient only if every influencing mutation participates,
including direct repositories and background jobs. An in-process planning lock
does not govern remote SurrealDB or independent clients.

The independent gate includes these deterministic interleavings:

1. A missing identity becomes present between planning and commit.
2. A unique match becomes ambiguous when another compatible identity appears.
3. Independent support is added/removed from a relation's slot after planning.
4. A read-only referenced entity changes type, Security or subjects without being
   included in the plan's changed rows.
5. Source desired head/lifecycle, space/config/provider approval or the exact
   worker attempt changes before publication.
6. Failure after first effect, before outbox or before receipt exposes no partial
   successful state; retry produces the complete same operation exactly once.
7. Existing `ingest_doc`, `extract_doc`, resolution and direct promotion use the
   same boundary as newly added source endpoints.

The agreed attempt authority is a dedicated `work_attempt` with a monotonically
increasing generation and unique token, surviving retries/archive restoration.
Earlier messages proposing resettable `WorkItem.attempts` are not authoritative.
Qualification must inspect the final active implementation and test stale work
from the same worker identity, not only different workers.

## Later independent source reviews

**Q08: restore must fence the complete consumer acknowledgment.** The original
wiki delivery loop read its cursor/batch before activity admission and advanced
the cursor after the callback's activity guard ended. Restore could therefore
separate an old-generation effect from its checkpoint. Recovery's shared
`outbox_delivery::deliver_batch` now encloses load, fetch, callback and checkpoint
CAS in the same admission. Both actual consumers call it. D's failed callback,
restore interleaving and owned-child SIGKILL tests retain this expectation; none
has executed yet.

**Q09: reversing a derived nested field must preserve independent siblings.** A
whole-object equality check left B's private nested contribution behind when a
later independent edit added another field. The D erase fixture publishes real
A/B sources, applies a fixed B-derived decision, then an independently authored
sibling and requires selective reversal. Integrity added recursive object
reversal and explicit Derived/IndependentMutation origins. The fixture uses
actual captured support for both operations. This source fix does not certify
new factual content supplied through a public edit; that is Q12.

**Q10: physical inventory pages disclose denied cardinality.** Surface's first
inventory route paged physical heads, removed unauthorized rows, then returned
the physical continuation. With limit 1 an inaccessible inventory produced one
empty page per hidden head. Surface accepted the source counterexample and
`13ce4a6` changes pagination to authorized rows with authorized lookahead; cursor
keys come only from returned rows. Lookahead observations participate in the
final policy check. A bounded scan fails explicitly without partial results.
D's actual-route tests require terminal empty inventory for a wholly denied
small dataset, retain real erased/retracted tombstones, and recheck clearance
between pages. This is source repair and unexecuted regression coverage.

**Q11: non-UUID erasure references need exact identity and a private guard.**
The initial ephemeral closure used substring matching, so an independent key
with a matching prefix or unrelated prose could be treated as owned. Erasure's
follow-up narrows native references to complete strings/object keys and parsed
nested JSON. The proposed durable guard initially hashed reference windows with
a retained public intent ID as salt. That does not prevent guessing low-entropy
identifying keys, and scanning windows is not exact typed reference matching.
Root, integrity and erasure received the counterexamples before acceptance.
Required cases distinguish native string `"42"` from integer `42`, preserve
prefix/suffix neighbors, normalize actual quoted/Unicode references, reject a
late producer with only the erased native reference, and preserve the guard
across old-backup replay/restart without retaining raw keys. Representation and
private key custody remain an explicit owner contract, not a D implementation
or qualified erasure claim.

**Q12: refreshing a row fingerprint cannot establish new entailment.** Actual
MCP `UpdateRelationshipHandler` merges a new `rel_type` into an existing Rel.
Its evidence checks establish the old Assertion/span, immutable bytes and ABAC,
but do not establish that the new relationship means the same thing as that
Assertion. REST accepts the corresponding full-record replacement.
`PublicMutation` captures old support and commits IndependentMutation, while
canonical maintenance refreshes existing support fingerprints to the changed
row. Current Rel reads retain the unchanged evidence reference. For example,
changing the published Oriole relationship from "opened" to "destroyed", keeping
the old "opened" assertion/quote and requesting Approved, lacks new source
support despite a fresh row hash. This is a concrete source counterexample,
not an executed failure. Owners/root must settle the retained correction
contract before a product change: preserve independent author input and harmless
annotations, but do not present changed factual content as supported by unchanged
old evidence. D does not require a model call on every public edit.

The request authorization cutoff is admission. Root explicitly permits an
already authorized request/tool operation to finish after bearer expiry or grant
revocation; subsequent operations reauthorize. D's signed REST/MCP tests check
that later boundary. Source/document envelope CAS, publication dependencies and
provider checks retain their separate stronger fences. A hypothesized in-flight
grant race is therefore not recorded as a defect under the agreed contract.

**Q13: a clipped evidence prefix can omit the claim's entire support.** Grounding
reported, and D independently inspected, `Assembly::evidence` in active scoped
search: it verifies the full assertion span, then returns only the first 1024
bytes while `Assembly::claim` returns the factual tuple. A note with neutral
preface followed by the opening fact therefore returns exact but irrelevant
bytes as the claim's evidence. D's actual publication/HTTP/MCP regression uses
a fixed 1.3 KB note with its sole support after byte 1024. Claim content must be
supported by the returned immutable quote, not merely by hidden storage bytes.
This remains authored, unexecuted coverage; semantic corpus thresholds are unchanged.

**Q14: partition-window exhaustion is not a complete empty result.** Grounding
reported, and D confirmed in the scoped semantic branch, that the single
400-candidate partition window ignores its `full` and `floor` information.
Candidates are filtered afterward, so 401 higher-scoring but unauthorized nodes
in an otherwise accessible partition can hide a lower-scoring authorized node.
The global 4096-candidate cap does not establish that each partition was exhausted.
Independent expectation: expand within a declared bound until the authorized
ranking is settled, or return an explicit bounded-work refusal; never report a
complete empty result merely because the first physical window was ineligible.
No lexical rescue or expected-source lookup may replace semantic ranking.

**Q15: an erasure inventory needs a captured write revision.** D reviewed the
proposed native shared guard-row touch and identified a separate transaction
boundary: inventory is prepared before the eventual purge transaction starts.
A different client can commit a WorkArtifact referencing only an owned opaque
extension key between those steps. Work artifacts intentionally do not advance
the canonical epoch. Touching the latest guard inside purge conflicts with
overlapping producers, but does not detect that already committed producer.
The old inventory can therefore omit payload even if purge wins every later
write conflict. A captured revision and transactional comparison detect the
stale inventory, but do not alone make a previously persisted deletion decision
safe: an independently changed owner could be permanently fenced by that stale
decision. Root's final protocol therefore reserves the Space under its original
durable intent before any inventory decision. Every ordinary payload mutation,
including operational rows and moves out of the Space, must refuse while held.
Writes committed before reservation enter inventory. The observed revision is
captured after reservation and original membership replay, then compared inside
purge; payload deletion, permanent membership projection and reservation release
commit atomically. Interrupted reservations resume their exact original intent;
missing original custody fails closed without a timeout release.

D reviewed A `2bff4abc862dd2683434dc4130b2760770990945` and E's
`5142e854` through `1a91ba9` source. The independent native-task test now requires
the exact reservation error, unchanged prestate and no late row before purge,
with the same update grammar succeeding before reservation. A real ordinary
snapshot taken during B's reservation after completed A erasure exercises replay
of the complete original-journal membership before independent inverse writes.
These are source-review findings; none establishes executed remote-server or
installed-artifact behavior. The shared database reservation is per Space, while
the existing same-client erasure barrier still drains the whole runtime.

**Q16: a caught mutation error must not permit a partial transaction commit.**
D found that guarded kernel deletes and ownership moves could change a row
before a later reservation/revision check returned an error. Ordinary callers
propagated that error, but a transaction closure that caught it and returned
success could commit the refused change. A `2bff4abc` records the first guard
failure and rolls the entire transaction back before index rebuilding or durable
commit, even if the closure returns success. It also captures the journal-bound
reservation permit once at transaction admission instead of consulting mutable
ambient context per mutation. The authored owner matrix covers caught errors,
earlier independent effects, revision exhaustion and sealed authority; D requested
the explicit move-out-of-reserved-space case because a move into a reserved
destination can fail before exercising the original postmutation defect.
The repair and regressions remain unexecuted in this review.
Successor A `b4ffdeccbf3532b0bca4264fa99320e20e9d1f39` adds typed and extension
move-out cases and requires the exact reservation/overflow error from both the
mutation and transaction. D reviewed these cases and closed that source-oracle
request. The same successor restricts the `source.erased` event exception to
original private erasure custody; an ordinary event label is not a bypass.

**Q17: the scheduler must retain the typed temporary-erasure failure.** At joined
`e6f9540bc683d5a5056b22b727f47ed9c812f057`, `classify_storage` lacks an arm for
`StorageError::ErasureInProgress` and returns `Terminal`. The actual preflight
handler then selects `RetryAction::Fail`. HTTP's retryable 503 mapping cannot
repair this worker policy. D requested a typed bounded retry and original-claim
regressions from recovery; unrelated terminal, stale-input and erased-source
failures must retain their own policy. This is a static source finding, not an
executed failure or proof that same-client global draining exposes every path.
Successor B `27d4937a9d1a07d90b2306bde1b94ac2c863f00a` adds the exact typed
transient arm and bounded preflight tests; D reviewed the source repair. It does
not infer retryability from incidental text. Runtime outcome review remains open.

**Q18: a public completion adapter must not adopt the current stored lease.**
Recovery identified, and D independently confirmed at `e6f9540`, that the retained
MCP complete handler loaded `leased_by` from the current row and used it as the
caller's authority. A delayed caller could therefore finish a successor attempt;
the claim response did not expose its actual committed token. The approved
contract adds the opaque token to claim results and requires its exact original
value on completion, plus current record read/write authorization and an atomic
comparison with the authorized envelope. D's independent public-dispatch oracle
covers same-worker reclaim, other-work tokens, malformed input, current scopes,
classification and named-space refusal with positive authorized completion.
B `5d98aea2d1c27c7337d00e699928a13f60880e3f` implements the token/envelope seam;
C's requested typed preflight/missing-row and checked-date followups are present
in B `5087427e8344b98645738828f08f80cd262bd8e0`, independently reviewed by D.
It checks both read/write scopes, returns the shared typed conflict for a missing
completion row, refuses zero duration and uses checked timestamp addition.
The authored public-attempt oracle still requires central execution.

Gateway review is separate from product erasure. The current gateway proposal
refuses opaque/stateful context and reserves every physical dispatch durably.
The independent offline contracts additionally require real HTTP framing
completion: `HTTPResponse.read1()` returning EOF does not by itself prove that
the declared Content-Length was received. An early stream completion event also
does not prove complete framing. This source-level issue was reported to the
gateway owner before executing candidate tests. The proposed byte allowance
plus protocol/item/tool constants still need concrete independent justification;
a review-hash field alone is not that proof, and no live budget is authorized.
The proposed provider token-count successor is reviewed separately in
`COUNTED_GATEWAY_REVIEW.md`; it does not retroactively qualify the current gateway.

Managed embedded WAL `39fccf3` and `f0b867b` were independently reviewed for
fsync-before-acknowledgment, rollback/poison behavior, checkpoint identity and
restore sequence isolation. No additional bounded protocol defect was identified
in that review. The actual SIGKILL oracles and erase live-file checks remain
necessary; whole-graph serialization/checkpoint cost under the large-input and
consumer workload is a separate future measurement. The reported approximately
140 OB3 workers over 51 minutes does not establish simultaneous requests.

## Evidence that remains unavailable

The Rust tests and Python oracle/collector source are authored. Both backend
execution, active commit fault injection, actual provider revocation dispatch,
physical deletion/restore replay, final native/container installation, real
semantic relevance, measured workload and consumer field acceptance are separate
unexecuted gates. The accepted wire shape is nested `results[].evidence` with
external `source_id`, a coverage object keyed by source and explicit partiality;
the earlier flat-result/coverage-array draft is superseded.
