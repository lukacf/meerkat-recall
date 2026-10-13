# Generated definition publication

A generated `KnowledgeViewDefinition` is a canonical effect. The planner captures
one named space's canonical epoch before reading inventory or invoking a provider,
keeps the complete material input rows, and prepares every definition in one exact
security partition before writing any of them. The existing canonical transaction
checks that epoch, original rows, original attempt when queued, and source fences;
it commits definitions, audit effects, outbox, decision, and receipt together.
Separate security partitions have separate receipts and explicit progress. No
multi-partition atomicity is implied.

`canonical_definition::bind_definition_decision` puts an address in the definition's
provenance chain. The address identifies the exact canonical receipt and output.
It is not proof by itself. Storage requires the address to identify the actual
commit, a Derived maintenance origin, and a nonempty original entity inventory.
The private `_definition_lineage` in that same receipt records complete generated
definitions and original material dependencies. It is not a public receipt field,
a new table, a synthetic source, or a second commit authority. Source-less derived
definitions still have a real `SourceDecisionReceipt` with empty source IDs and
conjunctive dependency IDs. The nil source used by generic control-row indexing is
not added to either typed receipt's source authority.

The effective input envelope uses the domain record's `HasSecurity` implementation
where one exists. In particular an Entity is its own subject. Original raw rows
remain unchanged for CAS and receipt evidence. Every definition must contain all
material input security levels, labels, handling, subjects, subject uncertainty,
and source provenance, including a material physical Source input itself even
when that Source has empty provenance. Selector roots and related article identities must
be actual observed Entities. Filter-only noncontributors remain protected against
preparation phantoms by the original epoch; they must not be invented as support.
Before-images of old definitions are exact CAS and inverse metadata, not content
authority for regenerated titles, selectors, or navigation.

`CanonicalRepository::definition_decision_inputs` follows only the current row's
exact pointer. It requires the actual receipt, matching original decision effect,
full output equality, and current published source authority. All non-Source
inputs and retained pre-head Sources require full original row equality including
bookkeeping. The version 2 private proof separately seals each headed input's
exact immutable `SourceRevision` selected by the original maintenance published
fence. Raw current Sources remain complete planning/CAS dependencies even when
they contain pending desired R2; sealed published R1 supplies content semantics.
The immutable revision control payload is captured before commit and checked in
the same transaction. Native present-control guards change only a private nonce,
which creates an actual write conflict; same-value UPDATEs are insufficient in
the pinned database engine.

Readback requires the actual current Active head to publish the same R1, the
retained immutable R1 to equal the whole sealed revision, and the current raw
Source authorization envelope to equal the original raw observation. Both raw
and published scopes and full source provenance must be contained by the output.
The helper returns the sealed R1 Source in that input slot. Its consumer must
load and authorize the actual current raw Source, project through normal current
read eligibility, authorize the published envelope, and compare that projection
to R1. It must never register a historical R1 row as the current raw observation.
Missing snapshots, unsupported private versions, policy changes, immutable input
tampering, and marker-only rows fail closed. Old proof data is not inferred or
migrated into publication evidence; regeneration requires a fresh inventory.
The helper does not scan history or confer read authority independently.

An exact visibility-only transition to Hidden may retain its old pointer while
changing only publication status and update time. It grants no new content proof;
the hidden after-image does not match the original proof. Any changed content or
republishing needs a fresh original inventory and new canonical receipt. Explicit
intrinsic definition configuration retains its existing authorized repository path.

Regression source covers both backends' complete-bundle rollback at three real
transaction fault points, source-less decision persistence, idempotent receipt
replay, forged pointers, changed inputs, direct output tampering, typed Entity
subjects, narrow hidden-only transitions, and physical Source input inclusion and
withdrawal under actual retained pre-head authority. These tests are authored for central
validation; no local Cargo validation was executed in the integrity worktree.

Additional pure regression source covers pending desired body changes, original
policy and lifecycle refusal, immutable revision/hash/incarnation identity, and
full published scope/provenance inclusion. Existing both-backend original-control
tests now verify an actual native nonce write while raw payload stays identical.
The real completed-R1/definition/pending-R2 public-read oracle is owned by the
pipeline/grounding qualification group and remains required; these unit checks do
not manufacture or claim a completed source publication.
