# Native transaction read guards

The pinned SurrealDB 2.6.5 engine's `Document::store_record_data` returns before
writing when `Document::changed()` is false. Table events also skip unchanged
records. Updating a payload or timestamp to the value it already has does not
create a write conflict. Deleting an absent record is not an absence fence.

`source_contact_lineage::OriginalControls` compares each originally present
control's exact space and raw payload. Existing metadata-only Source head and
operation rows receive a private changing nonce. Payload-bearing controls use
`fn::recall_reference_guard` with reference checking disabled after the original
comparison: this advances the real shared writer revision without updating a
historical payload. Their ordinary 039/040 CREATE/UPDATE/DELETE events touch the
same revision, including both spaces of a move. This covers schemafull completion
controls and permits inspection of historical payload while a restore's durable
erasure membership is already projected. Reservation checks remain active.
Unknown control tables refuse rather than assuming their writers participate.

The regression source checks actual native shared-revision increments across
two guard transactions, unchanged original payload, and refusal after payload
mutation. It does not claim an executed concurrent race result. The embedded
implementation compares originals under its exclusive mutation lock.

Canonical commits already perform a real per-space epoch write before ordinary
domain read checks. The 031 domain writer events conflict that row. WorkItem and
WorkRun guards write actual private hot/archive nonces. Present AuditLog,
SystemEvent and WorkArtifact inputs additionally advance the 039 shared reference
revision because they lack an unconditional canonical epoch event. New effect
rows use actual CREATE/RELATE writes at their physical keys. This is a call-site
audit of concrete protections, not a claim that a row comparison alone locks it.

Originally absent controls still use exact absence checks. Those checks need the
caller's existing actual predicate/epoch fence against concurrent creation; this
helper alone does not provide an absent-key write lock. Shared document/head and
manifest absence guards, including a foreign-space create at the same physical
ID, remain a separately coordinated source requirement. No placeholder domain
row or erased payload is created to simulate such a lock.

`PreparedDocGuards` additionally calls the version 043 database function
`fn::recall_physical_read_fence(record)` after checking each original head,
present/absent Doc and observed manifest. The function and every actual writer
receive the same typed native identity. The server hashes its complete native
spelling and uses three hexadecimal prefix digits in a bucket per allowed table
(`doc`, `source_head`, `source_publication_manifest`). The fixed 12,288 possible
rows contain only protocol version and a changing random nonce. They retain no
source, space, content, complete hash, or per-record history. Collisions cause
conservative transaction conflicts; they grant no read or write permission.

The erasure lane owns migration 043, writer events, dynamic installation,
restore classification, and the metadata-only head reservation event. The schema
lane owns registration/checksum parity. This consumer is not usable separately
from that coherent migration group. A missing function refuses the transaction.
Physical absence is checked across all spaces in both backends, including
malformed or foreign head/manifest rows at the original key. No domain record is
fabricated for an absence check; embedded mutations retain their exclusive lock.

Three further test methods cover actual foreign-space physical Doc appearance
on both backends and real native bucket changes while full Doc/head values stay
unchanged. The existing original Doc and manifest tests exercise their current
checks with the actual migration installed. These are authored, unexecuted
checks; a deterministic concurrent foreign-create/head/manifest oracle and
central runtime validation remain required before claiming concurrency coverage.

The deterministic overlap oracle uses the existing pinned database HTTP feature
as a storage **dev-dependency only**. Its database request pauses at a controlled
loopback HTTP barrier inside the original transaction after the read checks and
real metadata fence. The test-only native constructor allows just that exact
ephemeral `127.0.0.1:port`; production construction and network capabilities are
unchanged. An independent writer commits before releasing the observer. This
requires no timed sleeps, placeholder record, production hook or vendor patch.
The finalization lane owns the constructor and qualification owns the race test;
integration owns dependency-graph/lock verification and schedules execution. The
Cargo declaration alone does not demonstrate an executed overlapping transaction.

A canonical descendant can have several original source ancestors. Its document
guards use separate binding namespaces, so adding another source does not replace
the first source's head, documents or manifest observations. The native two-source
regression first commits both readsets, changes only the first original Doc, and
requires the combined guard to refuse while preserving the second Doc.

The storage test module now includes the independent five-case native overlap
oracle. The reviewed correction binds the original named space and executes the
observing transaction once, so an automatic conflict retry cannot issue a second
request to its single-use HTTP barrier. Actual outcomes remain unexecuted here.

Single-source guards also use the same namespace composer with an empty prefix,
which preserves their existing SQL binding names. The shared implementation
rejects accidental duplicate bindings in either mode. This keeps the active
single-source path and the multi-source canonical path on one composition rule;
no warning suppression or held-family dependency is needed. Existing default,
multi-source and independent native overlap test sources cover these entrypoints.

`DocRepository::exists_globally` exposes only whether an exact physical Doc ID
exists, including foreign-space or soft-deleted records. It rejects wrong table
identities and malformed read results. A scoped missing document therefore cannot
silently become the direct renderer's "no persisted input" fallback. The method
performs an existing embedded point read or native ID-only projection and never
mutates storage. This is a presence observation, not authorization or a transaction
fence; callers retain and revalidate their original input proof. Two authored
backend tests cover absence, foreign/deleted presence, unchanged original rows,
and physical deletion. No local Rust execution was performed.
