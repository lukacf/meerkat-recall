# Private erasure reference custody

The independent erasure journal retains one operating-system-generated 32-byte
reference key for its entire lifetime. Native reference tokens and the key's
fingerprint use the single pinned Recall HMAC implementation. The key is private
runtime custody, never database data, a receipt field, a public metadata field,
or a backup member. Debug output redacts it. The key and fingerprint files use
0600 permissions in a 0700 journal directory; symlink key files and permissive
private files are refused.

Fresh initialization durably publishes the key, fingerprint and empty membership
index before publishing the journal UUID anchor. A crash before that last step
can finish the same initialization only when all payload directories are empty.
An established journal always requires its original key, fingerprint and index,
even if no erasure has happened. Missing, foreign or inconsistent metadata blocks
opening; an old populated journal is never silently assigned a replacement key.
This is not a general journal migration or key-rotation facility.

Before physical purge, the erasure owner supplies the exact closure's opaque
reference tokens to `record_reference_membership`. The journal validates the
original committed intent, unions sorted lowercase 64-hex tokens, and publishes a
checksummed per-intent file followed by its durable index entry. Both writes use
same-directory temporary files, file fsync, rename and directory fsync. A separate
OS writer lock serializes independently opened handles. The membership contains
no source text, external identifiers, native record keys, table names or key
lengths. It binds the original journal, key fingerprint and exact erasure intent.

A crash after membership publication but before index publication is recoverable
from that complete file. Missing an acknowledged membership file, including an
explicitly empty membership, blocks readiness through the index. A corrupt or
foreign membership is never treated as an empty set. An intent recorded before
its first closure was planned may have no membership yet; recovery must plan it
under the existing exclusive erasure barrier before acknowledging purge.

These files survive database replacement, payload WAL retirement and bounded
backup deletion. Restore replays every retained membership, including erasures
whose source never existed in the restored backup. Erasure and restore callers
must project those memberships into the same fenced database transaction before
reporting completion or readiness. Journal primitives alone do not establish
that caller integration or runtime qualification.

Unit-test source covers stable reopen, redacted Debug, every missing established
anchor, refusal of legacy/populated journals, each pre-anchor crash boundary,
key substitution, monotonic opaque unions, malformed tokens and intent forgery,
lost empty memberships, membership/index crash ordering, corruption, foreign
journal membership, permissions and symlinks. Tests are centrally scheduled; this
document makes no test execution claim.

## Native query and restore custody

All four checked/unchecked StorageClient query constructors parse the query and
walk the native AST, including executable values inside caller bindings. Enabling
`OPTION IMPORT` is refused before dispatch. Quoted text and comments are not SQL
instructions, and explicitly disabling import remains valid. The unexported
snapshot replacement path alone permits import mode after verifying that its
handle retains an exclusive maintenance lease; it checks that custody before
removing the destination database.

The query constructors append the original key as native Bytes and the original
journal identity after caller bindings, so callers cannot accidentally replace
runtime custody. The pinned engine propagates those two names through isolated
function contexts. Raw SQL and database administrators remain trusted: this does
not prevent an administrator from deliberately returning a bound value. Query,
context and transport Debug redaction belongs to the paired engine/SDK patch;
operators must not enable logging of raw credential or query-binding envelopes.

Native startup performs a read-only version-1 and known-vector handshake before
storage admission. The probe uses a fixed non-secret key and independent literal
HMAC results for native numeric and string record IDs. HTTP and WebSocket remote
backends require the paired Recall server artifact; an unpatched server fails the
probe. The live journal anchor also binds the original private key fingerprint.
Ordinary service startup keeps the explicit migration/verification boundary and
never silently installs schema. The registered 039 migration establishes current
guards; public service readiness requires verified schema receipts.

Privileged native restore reinstalls current reference guards on all tables found
in the restored database, including historical extension tables, before erasure
replay and readiness. Ordinary native exports use the SDK's table selection to
exclude private membership; ordinary embedded exports use the kernel's backup
export API. Both retain the guard/count. Private embedded WAL checkpoints retain
the projection for local recovery. Neither ordinary export rewrites serialized
SQL or deletes private journal state.

Client test source covers all four query paths and caller binding replacement,
custom-function context inheritance, AST import rejection and harmless text,
unprivileged restore refusal, exact capability results, native export privacy,
historical-table guard reinstallation, and key-fingerprint substitution. These
are authored tests, not executed qualification. The erasure owner separately
must establish complete inventory, durable memberships and same-transaction
projection. Local admission or a same-row native transaction touch alone does
not prove completeness against a write committed between inventory and purge.

## Shared erasure reservation custody

A private active membership is written only after the erasure owner durably
reserves the owning space under the original journal intent. Inventory starts
after reservation. Every ordinary payload create, update or delete, including
operational rows and independent ownership changes, must refuse while another
owner holds that space. The original owner purges, projects permanent membership
and clears its reservation in one transaction. Failure retains the reservation;
there is no timeout that silently reopens writes. Recovery must resume an actual
reserved intent before processing other intents in that space.

The database reservation is per-space. The current runtime's erasure barrier is
broader: it drains all of that runtime's producers and blocks ordinary activity
until physical purge and its durable checkpoint finish. The per-space guard
therefore does not establish uninterrupted access to other spaces through that
same runtime. An independent client remains subject to its own admission state
and the database guards for the space it accesses.

`StorageClient::with_erasure_reservation` accepts an exact intent already present
in its original journal and requires an exclusive maintenance handle or bootstrap
replay authority. The scope is private Rust runtime context, never a header,
source metadata field or caller-provided query tag. A different nested intent is
refused. The native query helper captures the owner before dispatch and appends
`__recall_erasure_reservation` last; outside the matching original journal scope
it appends native `NONE`, overriding caller values. The paired core preserves
this third context name through isolated functions and redacts its Debug output.

Kernel transaction admission captures the scope through the original journal's
runtime adapter. The current EmbeddedStore mutation path executes the kernel
transaction synchronously in the admitted task; background tasks do not inherit
Tokio task-local custody. The journal-qualified accessor prevents another
journal's kernel from borrowing a scope. The authority must remain attached to
the originally admitted transaction if that execution path later moves threads.

Four additional regression definitions check original-intent and maintenance
requirements, nested foreign-scope refusal and error cleanup, native capture
across actual query dispatch with concurrent no-spoof behavior, and the real
kernel write path rejecting an unrelated task even through an escaped maintenance
store. These tests do not substitute for the erasure owner's complete reserved
inventory/fsync/purge crash oracles or installed paired-server qualification.

Reservation refusals are the dedicated storage `ErasureInProgress` variant on
both backends. The native driver promotes only the actual exact thrown sentinel
`publication:erasure_space_reserved` (or its known remote error wrapper), across
all four query methods. Non-destructive SDK error inspection preserves unrelated
unchecked statement errors and their positions. The HTTP/MCP adapter maps this
condition to the existing retryable `not_ready` response; ordinary generation
conflicts retain their separate meaning.

Native full-dataset restore parses the stored export before replacing the
database. The pinned exporter includes automatically generated array-child field
definitions; replaying their parents recreates placeholders before the saved
children are reached. Within exclusive restore custody, each unique top-level
saved field definition therefore uses `OVERWRITE`, preserving all of its actual
type/default/value/assertion/permission/readonly clauses. Duplicate explicit
(table, field) definitions are rejected before destructive replacement. No
fields or data statements are stripped, ordinary query behavior is unchanged,
and statement failures still keep restore behind its durable barrier. Source
regressions cover real native export/named restore with nested custom assertions,
full dataset replacement and original-journal continuity, plus duplicate-refusal
before deletion. These new regressions require central execution.
