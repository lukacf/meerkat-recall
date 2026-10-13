# Temporary write refusal during source erasure

The source erasure journal owns a per-space write reservation before reading the erasure inventory. Ordinary writes in that space, including deletes and moves out of or into it, must wait for cleanup. The database reservation leaves other spaces unfenced. The current runtime also drains its shared storage activity during physical purge and persistence, so requests through that same runtime can temporarily receive a readiness refusal even for another space. This is not a promise of uninterrupted per-space service during erase. A failed or interrupted cleanup retains its reservation for recovery of the same durable intent; it is not released by a timeout.

The public refusal uses the existing error contract: HTTP **503**, `{"error_code":"not_ready","message":"Source erasure is in progress; retry after cleanup","retryable":true}`. MCP returns the same body with `isError: true`. An actual source generation conflict remains HTTP 409 `conflict_generation`, nonretryable until the caller reconciles current state. A reservation refusal does not indicate a successful write or erase completion.

Storage preserves this as `StorageError::ErasureInProgress`, originating in the embedded kernel or the exact native database reservation error. API code does not search query strings for a sentinel. The native client must promote this refusal across checked and unchecked query paths while retaining unrelated statement errors and positions. These changes require the coherent private-reference migration 039, kernel, client and SDK group; this document and adapter tests alone do not qualify the concurrency or recovery protocol.

The shared HTTP/MCP error mapper and source/reindex/space adapters preserve this classification. Retained HTTP adapters can use the shared `legacy_storage_failure` projection to keep their legacy fields while preserving the canonical status, code and retry flag. Legacy adapters that convert storage failures into generic strings still require their owner’s typed conversion before exact public-response parity can be claimed. Backend rejection and rollback remain required regardless of response mapping.

Migration `039_private_reference_fence.surql` is registered immediately after `038_source_index_visibility.surql`. Its ordered checksum receipt is required by read-only startup schema verification. Only explicit migration applies it; an older, valid receipt prefix remains intact when upgraded. The final reviewed SQL source is Integrity 0c4e56247ba59afc01ee0d4f00dac68da5aeb792, SHA-256 2a33e12698ed04654a431bfe4aa4986b68f319e22bd7b076740f8085155b09b5. It restricts the completion-event exception to original private erasure custody and declares the required reference-key fingerprint field on the schema-full journal anchor table. Kernel admission hardening 2bff4abc862dd2683434dc4130b2760770990945 is also required. This supersedes the earlier unqualified d7d9e743 and b4ffdecc SQL checkpoints. Migration 034 remains unchanged; no deployed migration receipt is being rewritten. The native backend also requires the paired reference-token capability and the original private journal/key. Migration registration is not a restore, concurrency or erasure qualification receipt.


Migration `040_source_contact_intent.surql` follows 039. It stores sealed original
contact before/after authority for one physical source incarnation. This is
private **source payload**, not a payload-free control receipt or public wire
plan. It participates in canonical change fencing and the native erased-reference
guard on create/update/delete, including moves between spaces; the source
lifecycle erases it with that incarnation.

The registry requires the 040 checksum receipt before readiness, with no implicit
schema changes in ordinary serve. Explicit migration preserves the complete
earlier receipt prefix. Its reviewed source is Integrity
`35bd788`, SHA-256
`1db466ee4e95572d3c6ef74dc633a6cf77811b258d08efe362eabf714d021f89`.
The coherent contact acceptance, canonical publication, finalizer and erasure
group is required; a registered table or a source-only schema test does not
qualify publication or deletion. The separately owned private work-producer
origin follows in migration 041 as described below.

A verified earlier migration receipt prefix may be resumed when fresh
initialization has not yet written its journal anchor. This does not authorize
upgrading an established anchor that lacks the private reference-key
fingerprint. Such an anchor, or a mismatched/missing original journal, fails
closed even during explicit migration. The runtime does not invent a key,
rewrite an ambiguous identity or run a generic legacy conversion.


Migration `041_work_producer_origin.surql` follows 040 and is required by read-only
readiness verification. A's exact schema checkpoint is
`dc16e009d0c98892663a83aca1e86b901decb13e`, SHA-256
`f7d6fff57775ce1c4bbbfa295e0b685bf42ae0e33eee0b28c5c02ff3b3eaf364`.
Only explicit `migrate` installs it; ordinary serve does not apply DDL.
It stores the original private producer of a work item so a later retry cannot
acquire a different parent's budget or provenance. A missing row means unknown,
not direct work. This is source-derived operational data, not a retained
payload-free control exception: the ordinary reference guard, per-space
canonical epoch and physical erasure closure apply. The schema introduces no
public operation field, authorization grant, second work queue or monetary
ledger. Integration must join A's schema, B's repository and M/E's producer
consumers together before this source group is validated.
The final 041 source also fences the exact selection and source-intent artifact
membership used by reprocess acceptance. Changing a selected sibling or adding
an uncounted source intent invalidates the saved predicate; a later retry cannot
silently adopt that changed producer read set.

Creating a private producer-origin row or changing only its read nonce does not
advance the semantic canonical epoch. Deletion or any other producer field
change still invalidates the original space (and destination space for a move).
This permits prepared work to persist queue bookkeeping without replacing its
original epoch or weakening the exact producer and reference-guard checks.

Migration `042_watch_delegation.surql` follows 041 and stores the private original
recurring-watch delegation. Recovery's reviewed source is
`eaee3b9786a98e0f0520c6169653387964d39fff`, SHA-256
`041d4d282b9fe0b8a87524ed1ff0b9e0ec683a3ffffe96b4a68c50b7227a5f81`.
Its declared private `read_nonce` permits an actual native write conflict without
changing the semantic authority epoch. Every create/update/delete still uses the
original source-erasure reference guard. These rows retain source-derived
authority and must be physically erased with that source; they are not exempt
payload-free controls. Missing historical authority requires authorized
reconfiguration, never an inferred migration of old scheduler identities.

Migration `043_native_read_fence.surql` follows 042. Erasure's source is
`74e593918f580d58bfe562817d86be97d99e5896`, SHA-256
`ab27a84d340304e8674e0ef3309a2e923a0d4fe51b1e59c1b8e7b6c225eb7bb6`.
It coordinates native reads, including absence, with actual writers of `doc`,
`source_head` and `source_publication_manifest`. Readers and create/update/delete
events call the same database function with the actual typed record ID. The
function uses the first three hexadecimal characters of its SHA-256 to select
one of 4,096 fixed buckets per table. Each bucket stores only its ID, version and
changing nonce; it retains no source, space, complete identity hash or history.
The original row/absence checks remain mandatory. A colliding bucket may cause a
conservative conflict but grants no authority. Cross-space writers of the same
physical identity use the same bucket.

The 043 head reference event is metadata-only because erased heads retain
tombstone identities. It still checks the original private journal/key and
reservation and advances the shared writer revision. Historical guard refresh
must install the same rule. Restore admission and independent physical inspection
must accept only the exact bounded payload-free bucket shape; an unknown field
or malformed identity cannot gain an exemption merely from its table name.
The final schema validates an existing bucket before changing its nonce and
enforces that same exact shape on every create/update. Restore preflight checks
at most 12,289 boolean row projections before reinstalling any guards, refusing
more than 12,288 rows or any invalid shape. It never returns hidden payload or
silently cleans a malformed historical row. This supersedes the earlier
unqualified 5ba6ba6 schema, which lacked strict bucket admission.

The same migration installs the shared reference-writer events for the existing
`source_document_mapping` and `source_bundle_registration` rows. Their create,
update, delete and cross-space move paths advance the original shared reference
revision under the existing live-reference and erased-reference rules. Restore
refresh installs the same semantics. These rows do not add physical bucket
domains or change the strict three-table bucket limit.

Both ordered receipts are required by read-only readiness. Only explicit
`migrate` installs them, preserving all earlier receipts; ordinary `serve` does
not apply DDL. Source tests cover each missing receipt refusing readiness without
mutation, explicit sequential installation and idempotence. Registration alone
does not qualify delegation revocation, native races, restore classification or
physical erasure. The coordinated owner implementations and their actual
backend oracles must pass before those claims can be made.
