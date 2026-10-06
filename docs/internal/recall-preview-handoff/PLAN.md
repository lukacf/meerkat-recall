# Meerkat Recall preview implementation plan

This is the consumer-reviewed plan as of 6 October 2026. Read [README.md](README.md) for the handoff boundary, [AUDIT.md](AUDIT.md) for the original findings, and [SOURCE-STATUS.md](SOURCE-STATUS.md) for the exact source and transfer state. This document preserves the six-package preview recommendation; the earlier thirteen-package design was a broader maturity program.

**Implementation ownership:** Luka requires this plan to run under a different agent on another machine. The replacement agent and destination machine have not been selected. `elephant-release` is the source/evidence transfer contact, not the implementation owner, and must not implement or delegate this plan. Its already-started F01 work is transfer material, not permission to continue locally. Preserve that work and reconcile it before assigning overlapping repairs. `release-09-lead-local` has been informed of this boundary.

**Authorization and evidence:** publishing this handoff does not schedule implementation, qualification, deployment, data transition or adoption. The audit and this planning review were static only. No product builds, tests, linters, probes, exports or cutovers were performed by this review. The execution and acceptance work below is future work for the selected owner under the applicable authorization. Existing source-owner results retain their own provenance and limits.

## 1. Product boundary and decision status

The recommended preview is a self-hosted knowledge service that ingests documents, extracts source-backed claims and entities, and supports hybrid retrieval with authorized provenance. Core/Meta consumers can opt in. A standalone consumer can install and use Recall without adopting HomeCore, Core or Meta as its application platform. Internal use of Core does not impose a Core or Meta integration on consumers.

Accepted human constraints:

1. Multiple named spaces remain core, including per-space configuration, authorized administration, isolation and recovery. The default-space-only proposal was rejected.
2. Recall remains optional in the Meerkat family and independently usable. Optional startup must be real in consumer code.
3. General-purpose legacy Elephant migration, conversion and backward compatibility are not required. HomeCore can own a bespoke one-off transition. Existing useful aliases, repairs and migration safeguards need not be removed.
4. Implementation assignments should be substantial and independently executable, with each agent in a separate worktree and branch.
5. This plan belongs to a new agent on another machine, still to be selected.

The one-backend recommendation, wiki/editorial-view deferral, broader correction/restoration restrictions, transport allowlists and other functional cuts are **proposals, not blanket user-approved decisions**. Neither consumer's current feature use defines the independent product's scope. HomeCore's preferred cut retains extraction and promoted knowledge; a retrieval-only service would be a different product cut.

Correctness remains required for every retained capability. A finding affecting exposed behavior needs repair and appropriate qualification, or an explicitly selected and enforced capability restriction. A preview label, documentation warning, hidden tool or disabled adjudicator does not enforce a restriction. Refusal must cover public entry points, direct dispatch, ingestion requests, scheduling, worker claims and replay. Disabled consumers must not start in the background, and readiness must report the capability as absent.

| Area | Recommended preview boundary | Decision or limit |
| --- | --- | --- |
| Persistence | Single node using SurrealDB with embedded RocksDB, fresh stores, one supported backup/restore procedure. | One backend is a proposed operational simplification. ElephantDB parity and generic old-store conversion are outside that proposal. It does not reduce the number of supported spaces. |
| Spaces and security | Named spaces, authorized creation/administration, per-space settings, isolated ingestion/retrieval/provenance, and working recovery in every space. | Required. A separate hosted SaaS account/billing/control plane is not promised. |
| Knowledge | Real embeddings, extraction, grounded promotion, entities/claims, hybrid search and provenance; current-source revision replacement; an explicitly qualified set of temporal/supersession cases. | Wiki/editorial views, person-level purge and general correction/restoration remain proposed deferrals, not accepted cuts. |
| Deletion and retraction | Evidence that is deleted or retracted stops ordinary retrieval of records supported only by it. In-flight work cannot resurrect it. | Define the exact supported lifecycle allowlist. Disable a mutation if its full exposed semantics cannot be met. |
| Source erasure | Remove a selected source's live representations and exclusive derived support, fence stale work, and replay an independently durable erasure journal before restored readiness. | Required for OB3 real-data adoption, recommended for the preview, supported by HomeCore. If deferred, OB3 is synthetic qualification only. It is narrower than a person-discovery/purge engine. |
| Operations | All-space recovery, attempt fencing, truthful completion, capability-specific readiness, authorized backlog and honest usage labels. | Multi-instance embedding/library guarantees and precise currency accounting are proposed limits. Per-space configuration remains supported in one instance. |
| Consumers | Independent HomeCore and OB3 branches, documented standalone installation, and supported HTTP/MCP use. | No mandatory Recall crate/backend or MobKit dependency for OB3. Field adoption requires its own evidence. |

Fresh-store initialization must still fail honestly. The supported product needs usable backup/restore and safe retained operational commands. Removing generic legacy compatibility does not remove those obligations or establish that an existing transfer command is correct.

## 2. Consumer requirements

### HomeCore

HomeCore is the existing production consumer. It uses MCP and a native Darwin ARM64 binary under launchd. Its integration needs bounded MCP responses, meaningful error outcomes, non-interactive service-token provisioning, per-document security envelopes, and a consistent named quiesce/export interface. Its current health-based startup coupling must be replaced with an explicit optionality/readiness contract.

HomeCore proposes a new Recall store beside its old deployment, re-derivation through its source conveyor, preservation of source-less authoritative knowledge, qualification, then an explicit cutover. HomeCore owns that transition. Existing production continues independently during preparation; this plan does not start re-derivation or a cutover.

### OB3

OB3 wants a derived evidence service for source-backed claims/entities and hybrid retrieval. BigQuery remains its source of truth. OB3 owns fetching and access selection and pushes selected text/Markdown plus metadata through a thin public HTTP adapter; Recall does not crawl upstream systems for it. Stateless OB3 pods require a separate network service on GKE, not an embedded Recall store.

The first workflow needs multiple business scopes, sometimes containing the same source, with independent records and lifecycle per space. It needs service-principal grants, approved per-space processing connections, external source identity, immutable revision, source URL, capture metadata, classification, idempotent receipts and revision-aware query results. There is no implicit default-space or default-provider fallback.

OB3 owns source quality. Degraded/snippet text never replaces a full source; only a consumer-selected authoritative complete source is pushed as a replacement. Recall must not infer source ordering or quality from content length, a hash, or capture timestamps. OB3 falls back to its existing authorized source path when Recall evidence is unavailable, pending or stale, and counts that degradation.

Lifecycle signals are explicit. OB3 retracts under its deliberate removal/retirement policy or confirmed upstream deletion, and erases under an explicit erasure/confidential-content instruction. Transient fetch failure or `NO_PERMISSION` is neither. Persistent denial handling belongs to an explicit upstream policy. OB3 cannot detect every upstream ACL change in real time and reported a 30-minute polling interval. Once Recall receives a retract/erase instruction, it must suppress the affected visibility and fence work immediately while reporting remaining cleanup accurately.

OB3 reported approximately 140 concurrent analysis workers, source texts up to 1.1 million characters, and about 50 sources per scope per 30-minute pass, mostly unchanged. These are dated consumer reports, not Recall benchmarks. Use them to shape future fixtures; agree latency/timeout budgets before qualification.

OB3 initially does not need native MCP, PDF conversion, wiki/views, explicit cross-space search or historical browsing. It can query spaces independently and merge results. These are adapter limits, not authority to remove those features from Recall.

### Erasure and adoption

Omitting person entities does not remove personal content from raw documents, chunks, claims or embeddings. OB3 withdrew that proposed shortcut. OB3 can identify affected sources in its own authority and re-ingest corrected text, so its first workflow does not need a generic person-search/purge engine.

Real-data OB3 adoption requires source-scoped live erasure, preservation of independently supported knowledge, stale-work fencing, bounded backup retention and restore-time erasure replay. The journal must survive restoring an older data snapshot and contain no erased payload. Processor-side retention boundaries must be explicit. HomeCore supports this recommendation for household data too.

This is a consumer adoption requirement and scope recommendation, not user approval to implement it or an automatic universal publication gate. Synthetic OB3 qualification can proceed early if erasure is deferred; it must not be reported as production adoption.

## 3. Six substantial integration packages

Each package lead owns a complete reviewable result. Every implementing agent uses a separate worktree/branch; helpers integrate through the package lead. A-D join into one Recall candidate. E and F live in their consumer repositories and consume the public contract.

Sizes are relative planning judgments, not durations: M is bounded implementation, L is substantial end-to-end work, and XL has major coupled state/semantic uncertainty. Sizes include meaningful future verification. Provider throughput, data transition and field observation have elapsed-time constraints separate from implementation effort or token budget.

| Package | Size | Complete outcome |
| --- | --- | --- |
| A. Write-path integrity | XL | Grounding/security lineage, complete canonical publication/outbox, source-head CAS and revision replacement, retraction and recommended source-scoped live erasure. |
| B. Work, recovery and durability | L-XL | All-space leases/fencing/recovery, durable delivery/checkpoints/status, honest usage, fresh initialization, consistent quiesce/export, backup/restore and recommended erasure journal/replay barrier. |
| C. Public surface, operations and product | XL | Authorized spaces/configuration, token provisioning, HTTP/MCP schemas/errors, approved provider bindings, capability/readiness contracts, native Darwin ARM64 and container/PV artifacts. |
| D. Independent qualification | L | Independent semantic, integrity, authorization, provider, lifecycle and deployment oracles with separate HomeCore and OB3 fixtures. |
| E. HomeCore integration and transition | M-L | Optional startup, explicit household space, one-off inventory/delta/rollback, consumer acceptance and a finite field-observation gate. |
| F. OB3 integration and adoption | L | Public HTTP client, source-selection/ingest hook, source-head conflict handling, fallback accounting, fixtures and independent adoption gate. |

### A. Write-path integrity - XL

Own the canonical plan, operation receipt, event schema, effect idempotency and canonical schema changes. Carry resolved space and authority through lineage, operation identity, records, receipts and events. Grounding, security inheritance, atomic publication and retry reconciliation form one coherent result.

At commit, atomically check space ownership, current source revision/tombstone, policy preconditions and B's authoritative attempt generation. Reject stale publication even if the queue would later reject the attempt's completion. An existing assertion cannot stand for successful completion of all canonical effects, outbox and receipt. External source identity is space-local. Replacement/retraction retire obsolete support without destroying independent support.

One lead owns this coupled write path. Extra agents can own bounded modules, investigate, design counterexamples and independently review; do not create competing owners of canonical publication.

**Main risk:** this is the primary critical path. Fresh stores remove historical repair but do not remove partial writes, source-deletion races or stale-worker publication.

### B. Work, recovery and operational durability - L-XL

Own space-bound attempt/lease state, delivery and checkpoints. Discover durable pending/expired work independently of the current request or default space, including a quiet non-default space whose only work is stranded Running. Bind work to an explicit per-space configuration policy. A owns semantic effect identity; B acknowledges only durable successful effects.

Own fresh initialization and backup/restore mechanics usable on the supported persistent volume, coordinated with C packaging and qualified by D. The recommended erasure scope adds an independently durable journal and restore-readiness barrier. Status distinguishes retryable failure, terminal failure and completion. Accounting distinguishes reserved, measured, estimated and unknown usage and labels abstract budgets honestly; exact currency accounting is a separate scope decision.

Diagnostics and honest status can land early. Lease/fencing/recovery must join coherently with A, as must checkpointing, success-aware acknowledgment and idempotent handling. General migration/conversion infrastructure is not part of this package.

**Main risk:** queue ownership can appear correct while stale writes reach storage. A failed consumer must not advance a checkpoint. Retry policy and absent provider usage need explicit outcomes.

### C. Supported surface, operations and product - XL

Own the space registry, resolved configuration, authorized space selection/administration, service-token provisioning and capability contracts. B consumes the registry to discover work in every space; A validates current space existence/policy at commit. Vector partitions, caches, provider/model/dimension identity and extraction guidance respect the selected space and approved processors.

Publish public schemas for source ingestion, receipts, revision preconditions, search, status and typed errors across HTTP/MCP. Preserve per-document security envelopes and define document-versus-space guidance precedence. Approved model bindings apply to every outbound call and remain subject to current revocation.

Deliver the final native Darwin ARM64 artifact and container image, a single-replica StatefulSet/PV recipe, readiness/shutdown behavior and B-backed recovery instructions. Unsupported paths must be unavailable across entry points and background startup. New public names need no general legacy alias layer; existing useful aliases may remain.

Give registry/auth, public contracts/docs and container/deployment packaging substantial helper branches. Packaging consumes agreed operating interfaces and does not redefine canonical storage semantics. One package lead owns shared manifests and delivers the coherent checkpoint.

**Main risk:** C is the broadest coordination hotspot. Freeze grant/configuration/error contracts early. One desired replica does not prevent overlapping store writers during rollout. Packaging after acceptance changes the artifact being qualified, and route-only enforcement leaves background paths active.

### D. Independent qualification - L

Own expected outcomes and evidence independently of the implementation authors. Keep semantic and integrity verdicts separate even under one qualification lead. Tests must use the actual supported surfaces/backend, not replacement implementations that manufacture successful outcomes.

Parallel work can cover independent HomeCore/OB3 corpora, semantic expectations, authorization matrices, fault cases, provider validity and deployment acceptance. Expectation changes go through the independent lead. Begin contract fixtures immediately once future work is authorized; final gates use the integrated artifact rather than an HTTP stub.

**Main risk:** default-space household use cannot qualify multiple spaces or a different workload. Use distinct access/provider/model/dimension settings, interleaved work and quiet-space recovery. A small household corpus is biased; vector dimensions and variance alone do not prove meaningful embeddings. Qualification authorship alone is not a completed independent acceptance verdict.

### E. HomeCore integration and one-off transition - M-L

HomeCore is the proposed future consumer owner. Remove the Recall boot dependency, use an explicit household space, document the optional integration contract and qualify a fresh side-by-side store. Inventory re-derivable and source-less authoritative content, direct documents and canonical-only edits. Own the one-off disposition, write delta and rollback.

Use HomeCore worktrees/branches linked to the selected Recall contract. Existing production stays available until the separately authorized transition is qualified. There is no general Elephant compatibility obligation.

**Main risk:** re-derivation has provider, throughput and data-coverage uncertainty. Retaining the old store alone does not preserve later changes. Consumer code completion and field observation have different finish times.

### F. OB3 integration and first adoption - L

OB3 owns fetching, access selection and degraded-rendering policy with BigQuery as source authority. Implement the thin HTTP client, per-space source pushes, source-head CAS conflict handling, revision-aware query eligibility, fallback/degradation accounting and consumer acceptance. Consume only the public contract: no Recall storage/crate coupling and no MobKit change.

Use separate OB3 worktrees/branches. Client, fallback and fixtures can begin against the fixed public contract. Final fresh-store qualification follows the A/B integrity and C surface join, independently of HomeCore migration or observation. Real-data adoption additionally needs erasure/restore acceptance.

**Main risk:** a successful empty response can hide unavailable or stale evidence. Hashes do not order concurrent updates. Erasure can block real adoption despite a successful synthetic demonstration. Large inputs and high concurrency need measured budgets.

## 4. Critical shared contracts

Agree the small shared contract base before semantic implementations diverge. D and both consumer leads review concrete examples. One owner merges shared schemas/manifests; helper branches do not independently redesign shared types. This checkpoint is not a requirement to complete every implementation before parallel work begins.

### Source identity, revisions and acceptance - A/B/C with F

- External source identity is space-local. Define immutable revision, source URL, capture metadata and security fields.
- Recall returns an opaque source-head generation/ETag covering accepted desired revision and retractions. A content hash is not an ordering token, and `published_revision` alone is insufficient while a replacement is pending.
- Compare the expected head at acceptance and recheck it during publication. A newer accepted head immediately fences older attempts; only one attempt publishes that head.
- Expose desired revision, published revision, generation and operation/receipt. Identical retries return the original operation. Conflicting payload/security under the same identity yields an explicit conflict. Decide whether repeated observation timestamps are ignored or updated.
- On conflict, OB3 fetches the head and consults its authoritative source state. It does not need to invent its own monotonic counter.

### Publication, delivery and query coverage - A/B/C

- A owns canonical effect identity, complete publication, receipt and outbox. B owns attempt generation, delivery and acknowledgment/checkpoints. Attempt fencing must reach A's canonical transaction.
- Publish a revision and its eligible effects atomically. Define which required embeddings/claims must be searchable before the operation is complete.
- While a newer revision is pending, older evidence may be returned only as explicitly older, with published/desired revision and processing status. Never merge revisions of one source into one purportedly current answer.
- Every evidence result identifies its exact source revision. Support an exact source-revision constraint and explicit incomplete/not-ready outcomes. Per-space readiness does not prove every requested revision is current.
- A failed consumer does not advance its checkpoint; retry/deduplication and restart must preserve complete observable outcomes.

### Lifecycle and recommended erasure - A/B/C

- Replacement/retraction remove obsolete support while preserving independent current support. Publication rechecks current source lifecycle; stale attempts and delayed retries cannot revive retracted or erased content.
- The source-erasure contract covers live text, chunks, indexes/embeddings, exclusive claims/entities, provenance and work artifacts. Cover duplicated sources in every affected authorized space.
- A owns live deletion/support reconciliation. B owns the durable erasure journal and restore barrier. C exposes operation/status and the retention promise.
- The journal must survive restoring an older snapshot and contain no erased payload. Restored service cannot become ready before required replay.
- Define deliberate re-ingestion of corrected source text separately from retry. State backup retention and processor-side boundaries, and distinguish immediate visibility/fencing from physical-cleanup completion.

### Space, configuration and provider authority - C with A/B

- C owns the space registry/resolved-config read contract. B uses it for discovery/provider resolution; A uses it for commit-time existence/policy checks; D uses it for independent fixtures.
- Separate administration/provider grants from consumer ingest/read/retract grants. Provide non-interactive service-token provisioning and preserve per-document ABAC envelopes.
- Every outbound model operation, including query embedding, extraction and retries, resolves an approved per-space connection/account/endpoint/model binding. Missing or revoked approval blocks dispatch. Pinned job configuration cannot override later revocation.
- Provenance records safe binding/profile and policy identifiers, never credentials. Provider/model/dimension partitions and caches remain space-aware.
- Embedding identity includes model/version, dimensions and preprocessing, not dimensions alone. Freezing the embedding profile after indexing is a proposed small-preview rule. Changing a populated-space profile needs an explicit reindex/activation contract and remains a scope decision. Credential rotation is distinct.

### Errors, readiness and fallback - C with E/F

- Distinguish unauthorized, forbidden space, disabled capability, stale revision, unfinished processing, unavailable service and genuine zero matches on HTTP and MCP.
- Bound tool response size and latency. Fix health/readiness semantics and their consumer use explicitly; a health response must not make optional Recall a boot requirement.
- Consumer fallback uses only its own independently authorized source path and records degradation. It never changes Recall space, principal or provider to evade a refusal.

### Deployment and recovery boundary - B/C/D

- Agree ports, configuration, persistent paths, exclusive store ownership, drain/shutdown behavior, readiness and restore hooks before the packaging branch implements the service recipe.
- Name the supported quiesce/export command at this checkpoint so HomeCore's deployment procedure can consume it.
- Qualify native Darwin ARM64 and container/PV artifacts separately. A single desired replica alone does not prevent two embedded-store writers during rollout.
- Schema initialization and restore fail explicitly when required invariants cannot be established. Retained transfer commands refuse unsupported state and report partial work honestly; no generic legacy conversion framework is required.

## 5. Parallel worktree strategy and joins

The goal is substantial independent assignments, not a relay of small edits or maximum simultaneous authorship in shared files. Use extra agents for bounded modules, corpora, failure scenarios, authorization matrices, documentation, packaging and independent review. Giving conflicting branches one package name does not eliminate integration cost.

When the new owner is selected and implementation is authorized:

1. **Resolve and pin the base.** Read SOURCE-STATUS, preserve the transferred checkpoint, compare current upstream with the documented revisions, and select an exact shared base. Decide explicitly which existing dependency, PDF and authorization work enters it. Do not let a moving upstream silently change every worktree.
2. **Agree the small contract base.** Settle source-head/receipt/event, attempt/checkpoint, grant/configuration, lifecycle and error examples above. Assign shared-file/schema/manifests ownership and mechanical integration seams. D/E/F review the consumer-visible examples.
3. **Run A-F concurrently.** D develops independent oracles; E removes startup coupling and inventories transition content; F builds the client, source selection, fallback and fixtures; B advances diagnostics and backup groundwork; C runs packaging against fixed operating interfaces alongside public-surface work. No consumer branch changes shared canonical semantics.
4. **Join coherent revisions continuously.** A+B establish complete publication, source/attempt fencing, replay and visibility together. C's capability contract is consumed early by A/B. D qualifies public/background restrictions and the packaged candidate. Resolve failed interfaces at these joins, not in one final waterfall.
5. **Qualify the final named artifact.** Finish naming, packaging and docs before the exact-artifact gate. D's integrated gates precede final consumer acceptance, but do not delay early consumer code, fixtures or synthetic qualification. E and F then qualify the same candidate independently.

```text
Pinned source + owned public/state contracts
    |       |       |       |       |       |
    A       B       C       D       E       F
    +-- A/B integrity + C surface --+
                    |
         Final packaged artifact + D gates
                    |
            +-------+--------------------+
            |                            |
     E: HomeCore                    F: OB3
     transition/cutover             fresh-store qualification
     and observation                real adoption after erasure
            |                            |
            +---- separate receipts -----+
```

Cross-package invariants are concrete contract examples: resolved space and authority travel together; every operation has defined per-space configuration semantics; recovery discovers all spaces with durable work; attempt fences reach publication; deletion defeats in-flight publication; and disabled capabilities remain disabled during automatic work and replay.

HomeCore transition/observation does not serialize OB3 qualification and is not automatically a publication gate. Release evidence must accurately state which consumer and artifact gates completed. Neither a planned consumer branch nor a short demonstration establishes field adoption.

## 6. Future independent acceptance gates

These are future requirements, not claims that this review executed them. Existing HomeCore evidence supplies baselines and incident fixtures; it does not prove a new candidate passes. Record the actual source, artifact digest, supported backend/configuration, provider/model, fixture version, result and limitations for the acceptance being claimed.

| Gate | Required evidence |
| --- | --- |
| Real provider behavior and relevance | Real embeddings and extraction with provider/model provenance; a versioned known-answer query set with expected document/evidence identifiers, precision at an agreed cutoff and negative controls. Include the fake-vector failure mode. Dimensions/variance are necessary checks, not semantic proof. |
| Integrity and recovery | Interrupt canonical commit boundaries and verify complete effects/receipts after retry. Cover expired/stale attempts, deletion during extraction, duplicate events, failed consumers and restart across spaces. A quiet non-default space with only stranded Running work recovers without an unrelated request. |
| Spaces, auth and configuration | At least two named spaces with different content, policies, embedding providers/dimensions and extraction settings. Cover authorized administration, wrong-space/known-ID access, provenance isolation, interleaved work, independent provider/vector behavior and per-space readiness. Qualify space movement/deletion if exposed. |
| Restrictions | Exercise selected disabled capabilities through direct public calls, direct dispatch, queued work and automatic scheduling/replay. Family integration has authentication enabled. |
| Populated-store usefulness | Query and ingest under representative backlog with pending/failed/retryable work visible. Compare identical queries, corpus, route, provider/configuration and artifact. Historical HomeCore spot checks are not interchangeable performance baselines. |
| Semantics and lifecycle | Exact evidence spans, inherited visibility, atomic revision replacement, no mixed-revision answers, promised retraction/erasure behavior, pending replacement, delayed competing updates, retry after erasure/retraction, shared support and duplicate sources across spaces. Old backup restoration cannot expose erased content before journal replay. |
| Operations and independent use | Fresh install/schema initialization, non-interactive auth setup, consistent quiesce/export, backup/restore, exact artifact provenance, native Darwin ARM64 and container/PV shapes, HTTP/MCP, bounded output, optional startup and useful authorized fallback when Recall is absent/unready. |
| OB3-shaped workload | Two authorized spaces with distinct provider/profile settings; about 50 synthetic structured sources including large and shared inputs; no-op ingests; revision/retraction during extraction; wrong-space refusal; consumer withholding of degraded renderings; permission failure distinguished from retraction; approximately 140 concurrent searches within a budget agreed beforehand. Independent known-answer fixtures and an OB3 review batch completing through its existing path during Recall outage, with degradation counted. |

D owns the independent expectations and verdicts. Authored tests, stubbed contract fixtures and successful compilation are not final acceptance. Source-owner test selections must retain their actual exclusions; they cannot close unrelated semantic or failure-boundary findings.

HomeCore's proposed finite field gate is a cutover, an agreed number of consecutive normal-use days without integrity errors or lost ingests, and a known-answer rerun without regression. The day count is not agreed. OB3 synthetic qualification can run early; real adoption adds erasure/restore acceptance. Publication states completed evidence rather than implying both consumer gates have passed.

## 7. HomeCore's one-off transition obligations

HomeCore inventories and assigns a disposition to all authoritative content it intends to preserve. Directly ingested documents are only one category. Source-less entities, relationships, events, assertions, identity annotations and manual corrections must be accounted for where present, or their absence established.

The proposed procedure is a fresh store beside the old one, re-derivation through existing sources, explicit handling of source-less content, qualification, then a separately authorized cutover. At cutover, capture or stop all relevant writes, reconcile the final delta, switch service selection and verify rollback behavior. The delta includes updates, deletions/retractions and policy/metadata changes, not just newly created rows. A bespoke journal or a bounded write freeze with reconciliation may serve the one-off procedure.

Keeping the old store alone does not preserve changes made only to the new system after cutover. HomeCore's runbook must address those changes. The two specific corrections from consumer review were a complete source-less-content inventory and a mutation-complete delta; neither requires generalized compatibility infrastructure.

HomeCore accepts that old document/assertion IDs in transcripts and memories become historical. Agents re-find evidence through search; an operator-side map may retain old source/hash correspondence. The preview does not promise old-ID resolution or an agent-facing compatibility lookup.

Source-index document count, derived-document count and embedded-chunk count are different measurements. No transition duration or price is established without the chosen corpus and measured throughput. Agent/token capacity does not remove provider quotas, storage throughput or observation time.

## 8. Source provenance and existing work to reuse

The original audit remains pinned to [7f8446856b3a9eb7b74976a406536e90d09c1cf6](https://github.com/lukacf/meerkat-recall/tree/7f8446856b3a9eb7b74976a406536e90d09c1cf6). Its findings must be reconciled against successor work, not silently relabeled as a fresh audit of current main.

The subsequent [PDF change in PR #21](https://github.com/lukacf/meerkat-recall/pull/21), at [5119f0597753dfb68206111e9fd9aad457be6802](https://github.com/lukacf/meerkat-recall/tree/5119f0597753dfb68206111e9fd9aad457be6802), was inspected statically. It replaces PDF text extraction and changes packaging inputs; it does not close trust, canonical commit, recovery or usage findings. Preserve the patched vendored parser and notices. It is not an OCR engine, and future PDF qualification must exercise this replacement.

[PR #22](https://github.com/lukacf/meerkat-recall/pull/22) was reviewed narrowly at [dd2d96672e869e2c2b1247f703312b5e0f9637af](https://github.com/lukacf/meerkat-recall/tree/dd2d96672e869e2c2b1247f703312b5e0f9637af), based on `5119f059`. It subsequently merged at [f59e6f5c99ed095a015656626f4d32c106c13075](https://github.com/lukacf/meerkat-recall/commit/f59e6f5c99ed095a015656626f4d32c106c13075). SOURCE-STATUS records the latest verified state and evidence qualifications; this plan is not a new full audit of that merge.

Reuse PR22's shared HTTP/MCP token/context work, ordinary entity-envelope CAS, retention retry/archive repairs, migration preflight, native executable/version/alias, installation/provenance work, bounded CLI readiness check and Core dependency integration. Useful implementation pointers are [entity CAS](https://github.com/lukacf/meerkat-recall/blob/dd2d96672e869e2c2b1247f703312b5e0f9637af/crates/storage/src/entity_repository.rs#L407), [CLI readiness](https://github.com/lukacf/meerkat-recall/blob/dd2d96672e869e2c2b1247f703312b5e0f9637af/bin/elephant/src/main.rs#L415), and the [deployment boundary](https://github.com/lukacf/meerkat-recall/blob/dd2d96672e869e2c2b1247f703312b5e0f9637af/docs/operations/deployment.mdx#L7).

These reduce implementation work but do not complete A/B/C. Entity CAS is not source-head CAS. The inspected delta did not implement canonical multi-effect publication, grounding/general lease repairs, source-revision/erasure contracts, restore-time erasure replay, or all-model-call approved provider dispatch/revocation. Its production readiness logic was unchanged; its deployment guide still required separate production configuration. Migration preflight bounds part of F08 without removing retained fresh-initialization and command-correctness obligations.

Preserve the later F01 transfer checkpoint described in SOURCE-STATUS. It is not a qualified repair merely because source edits exist. Pin and reconcile it before assigning overlapping work; do not discard dirty files, assume a passing test run, or silently extend the old agent's mandate.

The source owner reported selected deterministic validation on the inspected candidate. Such evidence is useful within its recorded scope; no source-owner or PR-body result was a run by this static reviewer, and it does not establish exact native/container acceptance or consumer production adoption. Do not reopen repaired subcases solely because the audit predates them, and do not close entire findings from a selected green suite.

Consumer requirements came from the 6 October HomeCore/OB3 review and follow-up confirmations. OB3 reported source `ce5bd68`, product 3.16.1, Core 0.8.51 and MobKit 0.8.47. Its operating figures are reported evidence, not independently rerun measurements. The substantive agreements are recorded in this document so another machine does not need access to private message files.

## 9. Bounded decisions before implementation or qualification

- Select the replacement agent/machine and reconcile the exact source/transfer base before assigning work.
- Explicitly select any proposed functional deferrals, backend restriction and lifecycle allowlist. Consumer non-use alone is not approval.
- Settle the public operation/error schemas, source-head/receipt examples and exact source-erasure retention/restore promise, including processor boundaries.
- Decide populated-space embedding-profile behavior and the supported reindex/activation boundary, separately from credential rotation.
- Name the supported consistent quiesce/export command and the operating interfaces required by both artifact shapes.
- Agree comparable, version-pinned latency/timeout and output-size budgets before performance qualification.
- Agree HomeCore's finite field-observation count and incorporate the complete inventory/mutation-delta corrections into its one-off runbook.

None of these decisions reinstates generalized legacy Elephant compatibility. The current plan is a portable handoff and recommended execution design, not an active assignment or an implementation/deployment record.
